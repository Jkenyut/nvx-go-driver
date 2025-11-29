package database

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Jkenyut/nvx-go-driver/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PGXClient struct {
	Pool *pgxpool.Pool
	Cfg  config.SQLConfig
}

func NewPGXClient(cfg config.SQLConfig) (*PGXClient, error) {
	cfg = cfg.WithDefaults()

	if !cfg.Enable {
		return nil, fmt.Errorf("database is disabled")
	}

	dsn := buildDSN(cfg)

	if !cfg.AutoReconnect {
		pool, err := connectOnce(cfg, dsn)
		if err != nil {
			return nil, err
		}
		return &PGXClient{Pool: pool, Cfg: cfg}, nil
	}

	return autoReconnect(cfg, dsn)
}

func buildDSN(cfg config.SQLConfig) string {
	if cfg.Connection != "" {
		return cfg.Connection
	}

	return fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?%s",
		cfg.Username,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		cfg.Database,
		cfg.Options,
	)
}

func connectOnce(cfg config.SQLConfig, dsn string) (*pgxpool.Pool, error) {
	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}

	// === Pool tuning ===
	if cfg.CustomPool {
		poolConfig.MaxConns = int32(cfg.MaxConn)
		poolConfig.MinConns = int32(cfg.MinConn)
		poolConfig.MaxConnLifetime = time.Duration(cfg.LifeTime) * time.Minute
		poolConfig.MaxConnIdleTime = time.Duration(cfg.MaxConnIdleTime) * time.Second
		poolConfig.HealthCheckPeriod = time.Duration(cfg.HealthCheckPeriod) * time.Second
	}

	// do NOT duplicate timeout in context
	poolConfig.ConnConfig.ConnectTimeout = time.Duration(cfg.ConnectTimeout) * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), poolConfig.ConnConfig.ConnectTimeout)
	defer cancel()

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, err
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}

	return pool, nil
}

func autoReconnect(cfg config.SQLConfig, dsn string) (*PGXClient, error) {
	delay := time.Duration(cfg.StartInterval) * time.Second

	for attempt := 1; attempt <= cfg.MaxError; attempt++ {
		pool, err := connectOnce(cfg, dsn)
		if err == nil {
			log.Printf("[PGX] Connected")
			return &PGXClient{Pool: pool, Cfg: cfg}, nil
		}

		log.Printf("[PGX] Failed (%d/%d): %v", attempt, cfg.MaxError, err)

		if attempt == cfg.MaxError {
			return nil, err
		}

		time.Sleep(delay)

		if delay < 30*time.Second {
			delay *= 2 // cap at 30
		}
	}

	return nil, fmt.Errorf("autoReconnect unreachable")
}

func (c *PGXClient) Close() {
	if c.Pool != nil {
		c.Pool.Close()
	}
}
