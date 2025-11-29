package database

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/Jkenyut/nvx-go-driver/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PGXClient struct {
	pool atomic.Value // stores *pgxpool.Pool
	Cfg  config.SQLConfig

	AfterConnect func(ctx context.Context, conn *pgx.Conn) error
}

// -------------------- Constructor --------------------

func NewPGXClient(cfg config.SQLConfig) (*PGXClient, error) {
	return NewPGXClientWithHook(cfg, nil)
}

func NewPGXClientWithHook(cfg config.SQLConfig, afterConnect func(ctx context.Context, conn *pgx.Conn) error) (*PGXClient, error) {
	cfg = cfg.WithDefaults()

	if !cfg.Enable {
		return nil, fmt.Errorf("database is disabled")
	}

	// Ensure defaults
	if cfg.MaxConn == 0 {
		cfg.MaxConn = runtime.NumCPU() * 4
	}
	if cfg.MinConn == 0 {
		cfg.MinConn = runtime.NumCPU()
	}
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = 5
	}
	if cfg.HealthCheckPeriod == 0 {
		cfg.HealthCheckPeriod = 30
	}
	if cfg.StartInterval == 0 {
		cfg.StartInterval = 2
	}
	if cfg.MaxError == 0 {
		cfg.MaxError = 5
	}

	dsn := buildDSN(cfg)

	client := &PGXClient{
		Cfg:          cfg,
		AfterConnect: afterConnect,
	}

	// Startup connect
	var (
		pool *pgxpool.Pool
		err  error
	)

	if cfg.AutoReconnect {
		pool, err = connectWithRetry(cfg, dsn, afterConnect)
	} else {
		pool, err = connectOnce(cfg, dsn, afterConnect)
	}

	if err != nil {
		return nil, err
	}

	client.setPool(pool)

	// Background monitor
	go client.monitorConnection()

	return client, nil
}

// -------------------- DSN --------------------

func buildDSN(cfg config.SQLConfig) string {
	if cfg.Connection != "" {
		return cfg.Connection
	}
	if cfg.Options == "" {
		return fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
			cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.Database)
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?%s",
		cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.Database, cfg.Options)
}

// -------------------- Atomic Pool --------------------

func (c *PGXClient) Pool() *pgxpool.Pool {
	p := c.pool.Load()
	if p == nil {
		return nil
	}
	return p.(*pgxpool.Pool)
}

func (c *PGXClient) setPool(p *pgxpool.Pool) {
	c.pool.Store(p)
}

// -------------------- Pool Creation --------------------

func connectOnce(cfg config.SQLConfig, dsn string, afterConnect func(ctx context.Context, conn *pgx.Conn) error) (*pgxpool.Pool, error) {
	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	// Pool settings
	if cfg.MaxConn > 0 {
		poolConfig.MaxConns = int32(cfg.MaxConn)
	}
	if cfg.MinConn > 0 {
		poolConfig.MinConns = int32(cfg.MinConn)
	}
	if cfg.LifeTime > 0 {
		poolConfig.MaxConnLifetime = time.Duration(cfg.LifeTime) * time.Minute
	}
	if cfg.MaxConnIdleTime > 0 {
		poolConfig.MaxConnIdleTime = time.Duration(cfg.MaxConnIdleTime) * time.Second
	}
	if cfg.HealthCheckPeriod > 0 {
		poolConfig.HealthCheckPeriod = time.Duration(cfg.HealthCheckPeriod) * time.Second
	}

	poolConfig.ConnConfig.ConnectTimeout = time.Duration(cfg.ConnectTimeout) * time.Second

	if poolConfig.ConnConfig.RuntimeParams == nil {
		poolConfig.ConnConfig.RuntimeParams = map[string]string{}
	}
	poolConfig.ConnConfig.RuntimeParams["application_name"] = "nvx-go-driver"

	// Hook
	if afterConnect != nil {
		poolConfig.AfterConnect = afterConnect
	}

	ctx, cancel := context.WithTimeout(context.Background(), poolConfig.ConnConfig.ConnectTimeout)
	defer cancel()

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("new pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}

	return pool, nil
}

func connectWithRetry(cfg config.SQLConfig, dsn string, hook func(ctx context.Context, conn *pgx.Conn) error) (*pgxpool.Pool, error) {
	delay := time.Duration(cfg.StartInterval) * time.Second

	for attempt := 1; attempt <= cfg.MaxError; attempt++ {
		pool, err := connectOnce(cfg, dsn, hook)
		if err == nil {
			log.Println("[PGX] Connected")
			return pool, nil
		}

		log.Printf("[PGX] Connect failed (%d/%d): %v", attempt, cfg.MaxError, err)

		if attempt == cfg.MaxError {
			return nil, err
		}

		time.Sleep(delay)
		if delay < 30*time.Second {
			delay *= 2
		}
	}

	return nil, errors.New("connectWithRetry unreachable")
}

// -------------------- Monitor --------------------

func (c *PGXClient) monitorConnection() {
	cfg := c.Cfg
	dsn := buildDSN(cfg)

	interval := time.Duration(cfg.HealthCheckPeriod) * time.Second
	if interval <= 0 {
		interval = 10 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		pool := c.Pool()
		if pool == nil {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := pool.Ping(ctx)
		cancel()

		if err == nil {
			continue
		}

		log.Printf("[PGX][WARN] unhealthy pool: %v", err)

		newPool, err := connectWithRetry(cfg, dsn, c.AfterConnect)
		if err != nil {
			log.Printf("[PGX][WARN] reconnect failed: %v", err)
			continue
		}

		old := c.Pool()
		c.setPool(newPool)

		if old != nil {
			go old.Close()
		}

		log.Println("[PGX][INFO] pool swapped")
	}
}

// -------------------- Close --------------------

func (c *PGXClient) Close() {
	p := c.Pool()
	if p != nil {
		p.Close()
	}
}
