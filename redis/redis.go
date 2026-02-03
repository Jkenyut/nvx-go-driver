package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Jkenyut/nvx-go-driver/config"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
)

// Client wraps go-redis client with observability, automatic reconnect,
// and standard configuration.
type Client struct {
	client *redis.Client
	cfg    config.RedisConfig
	log    *zerolog.Logger
}

// Metrics provides Prometheus-compatible metric functions.
type Metrics struct {
	HitsTotal     func() float64
	MissesTotal   func() float64
	TimeoutsTotal func() float64
	TotalConns    func() float64
	IdleConns     func() float64
	StaleConns    func() float64
}

// NewClient creates a new Redis client with the provided configuration.
// It applies sensible defaults if specific fields (Host, Port, PoolSize) are missing.
//
// Defaults applied:
//   - Host: "127.0.0.1" (if empty)
//   - Port: 6379 (if 0)
//   - PoolSize: 10 (if 0)
//   - MinIdleConn: 5 (if 0)
//   - PoolTimeout: 30s (if 0)
//
// Usage Example:
//
//	cfg := config.RedisConfig{
//	    Enable: true,
//	    Host:   "localhost",
//	    // Port defaults to 6379
//	}
//	client, err := redis.NewClient(cfg, logger)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer client.Close()
//
//	val, err := client.Client().Get(ctx, "key").Result()
func NewClient(cfg config.RedisConfig, logger *zerolog.Logger) (*Client, error) {
	if logger == nil {
		nop := zerolog.Nop()
		logger = &nop
	}

	if !cfg.Enable {
		return nil, errors.New("redis disabled in config")
	}

	cfg = applyDefaults(cfg)

	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)

	rdb := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     cfg.Password,
		DB:           0, // default DB
		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.MinIdleConn,
		MaxIdleConns: cfg.MaxIdleConn,
		DialTimeout:  time.Duration(5) * time.Second,
		ReadTimeout:  time.Duration(cfg.PoolTimeout) * time.Second,
		WriteTimeout: time.Duration(cfg.PoolTimeout) * time.Second,

		// go-redis handles reconnects automatically
	})

	// Verify connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		rdb.Close()
		return nil, fmt.Errorf("redis connection failed: %w", err)
	}

	logger.Info().
		Str("addr", addr).
		Int("pool_size", cfg.PoolSize).
		Msg("Redis connected successfully")

	return &Client{
		client: rdb,
		cfg:    cfg,
		log:    logger,
	}, nil
}

func applyDefaults(cfg config.RedisConfig) config.RedisConfig {
	if cfg.Host == "" {
		cfg.Host = "127.0.0.1"
	}
	if cfg.Port == 0 {
		cfg.Port = 6379
	}
	if cfg.PoolSize == 0 {
		cfg.PoolSize = 10
	}
	if cfg.MinIdleConn == 0 {
		cfg.MinIdleConn = 5
	}
	if cfg.PoolTimeout == 0 {
		cfg.PoolTimeout = 30
	}
	return cfg
}

// Client returns the underlying *redis.Client for direct usage.
func (r *Client) Client() *redis.Client {
	return r.client
}

// Close gracefully closes the Redis client connection.
func (r *Client) Close() error {
	r.log.Info().Msg("Closing Redis client")
	return r.client.Close()
}

// Set executes a simplified Redis SET command.
// expiration of 0 means no expiration.
func (r *Client) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	return r.client.Set(ctx, key, value, expiration).Err()
}

// Get executes a simplified Redis GET command.
// Returns redis.Nil error if key does not exist.
func (r *Client) Get(ctx context.Context, key string) (string, error) {
	return r.client.Get(ctx, key).Result()
}

// Del executes a simplified Redis DEL command.
func (r *Client) Del(ctx context.Context, key string) error {
	return r.client.Del(ctx, key).Err()
}

// Metrics returns observability metrics for the Redis pool.
func (r *Client) Metrics() Metrics {
	return Metrics{
		HitsTotal: func() float64 {
			stats := r.client.PoolStats()
			return float64(stats.Hits)
		},
		MissesTotal: func() float64 {
			stats := r.client.PoolStats()
			return float64(stats.Misses)
		},
		TimeoutsTotal: func() float64 {
			stats := r.client.PoolStats()
			return float64(stats.Timeouts)
		},
		TotalConns: func() float64 {
			stats := r.client.PoolStats()
			return float64(stats.TotalConns)
		},
		IdleConns: func() float64 {
			stats := r.client.PoolStats()
			return float64(stats.IdleConns)
		},
		StaleConns: func() float64 {
			stats := r.client.PoolStats()
			return float64(stats.StaleConns)
		},
	}
}

func (r *Client) Nil() redis.Error {
	return redis.Nil
}
