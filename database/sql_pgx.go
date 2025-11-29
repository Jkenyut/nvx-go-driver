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

	var (
		client *PGXClient
		err    error
	)

	// ≤==== Mode tanpa autoreconnect di awal (startup only) ====>
	if !cfg.AutoReconnect {
		pool, err := connectOnce(cfg, dsn)
		if err != nil {
			return nil, err
		}

		client = &PGXClient{
			Pool: pool,
			Cfg:  cfg,
		}

		// Background monitor untuk runtime reconnect
		go client.monitorConnection(cfg, dsn)
		return client, nil
	}

	// ≤==== Mode startup dengan autoreconnect ====>
	client, err = autoReconnect(cfg, dsn)
	if err != nil {
		return nil, err
	}

	// Tetap jalankan monitor runtime
	go client.monitorConnection(cfg, dsn)

	return client, nil
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

	if err = pool.Ping(ctx); err != nil {
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

func (c *PGXClient) monitorConnection(cfg config.SQLConfig, dsn string) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := c.Pool.Ping(ctx)
		cancel()

		if err == nil {
			continue // DB sehat
		}

		log.Printf("[PGX][WARN] Lost DB connection: %v", err)

		// Tutup pool lama
		c.Pool.Close()

		// Coba reconnect
		var pool *pgxpool.Pool
		for {
			pool, err = connectOnce(cfg, dsn)
			if err == nil {
				break
			}

			log.Printf("[PGX][RECONNECT] Retry in %d sec…", cfg.StartInterval)
			time.Sleep(time.Duration(cfg.StartInterval) * time.Second)
		}

		log.Println("[PGX] Reconnected successfully!")

		// Replace pool secara atomik
		c.Pool = pool
	}
}
