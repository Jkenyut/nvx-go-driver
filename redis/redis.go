// Package redis provides a Redis client implementation.
package redis

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"time"

	"github.com/Jkenyut/nvx-go-driver/config"
	"github.com/bytedance/sonic"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
)

// ErrClientNil is returned when an operation is attempted on a nil client.
var ErrClientNil = errors.New("redis client is nil")

// Client wraps go-redis client with observability, automatic reconnect,
// and standard configuration.
type Client struct {
	client *redis.Client
	cfg    *config.RedisConfig
	log    *slog.Logger
}

// Metrics provides Prometheus-compatible lazy metric functions.
// Each field is a function that returns the current value when called,
// ensuring Prometheus scrapers always get up-to-date data.
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
func NewClient(cfg *config.RedisConfig, logger *slog.Logger) (*Client, error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	if !cfg.Enable {
		return nil, errors.New("redis disabled in config")
	}

	cfg = cfg.WithDefaults()

	var opt *redis.Options
	if cfg.Connection != "" {
		parsedOpt, err := redis.ParseURL(cfg.Connection)
		if err != nil {
			return nil, fmt.Errorf("invalid redis connection url: %w", err)
		}
		opt = parsedOpt
	} else {
		opt = &redis.Options{
			Addr:     net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)),
			Password: cfg.Password,
			DB:       cfg.Database,
		}
	}

	appName := cfg.ApplicationName
	if appName == "" {
		appName = "nvx-go-driver"
	}

	opt.ClientName = appName
	opt.TLSConfig = redisTLSConfig(cfg)
	opt.PoolSize = cfg.PoolSize
	opt.MinIdleConns = cfg.MinIdleConn
	opt.MaxIdleConns = cfg.MaxIdleConn
	opt.ConnMaxLifetime = time.Duration(cfg.ConnMaxLife) * time.Second
	opt.DialTimeout = time.Duration(cfg.ConnectTimeout) * time.Second
	opt.PoolTimeout = time.Duration(cfg.PoolTimeout) * time.Second

	rdb := redis.NewClient(opt)

	if cfg.EnableTelemetry {
		if err := redisotel.InstrumentTracing(rdb); err != nil {
			return nil, fmt.Errorf("failed to instrument redis tracing: %w", err)
		}
		if err := redisotel.InstrumentMetrics(rdb); err != nil {
			return nil, fmt.Errorf("failed to instrument redis metrics: %w", err)
		}
	}

	var err error
	maxAttempts := 1
	if cfg.AutoReconnect {
		if cfg.MaxError > 0 {
			maxAttempts = cfg.MaxError
		} else {
			maxAttempts = 5
		}
	}

	for i := 1; i <= maxAttempts; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.ConnectTimeout)*time.Second)
		err = rdb.Ping(ctx).Err()
		cancel()

		if err == nil {
			break
		}

		if i < maxAttempts {
			logger.Warn("Redis connection failed, retrying...",
				"error", err,
				"attempt", i,
				"max_attempts", maxAttempts)
			time.Sleep(time.Duration(cfg.StartInterval) * time.Second)
		}
	}

	if err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("redis connection failed after %d attempts: %w", maxAttempts, err)
	}

	logger.Info("Redis connected successfully",
		"addr", opt.Addr,
		"pool_size", cfg.PoolSize)

	return &Client{
		client: rdb,
		cfg:    cfg,
		log:    logger,
	}, nil
}

func redisTLSConfig(cfg *config.RedisConfig) *tls.Config {
	if !cfg.TLS {
		return nil
	}
	return &tls.Config{
		ServerName:         cfg.Host,
		InsecureSkipVerify: cfg.InsecureSkipVerify,
	}
}

// Client returns the underlying *redis.Client for direct usage.
func (r *Client) Client() *redis.Client {
	if r == nil || r.client == nil {
		return nil
	}
	return r.client
}

// Close gracefully closes the Redis client connection.
func (r *Client) Close() error {
	if r == nil || r.client == nil {
		return ErrClientNil
	}
	r.log.Info("Closing Redis client")
	return r.client.Close()
}

// Ping verifies the connection to Redis is alive.
// Useful for application health check endpoints.
func (r *Client) Ping(ctx context.Context) error {
	if r == nil || r.client == nil {
		return ErrClientNil
	}
	return r.client.Ping(ctx).Err()
}

// Set executes a simplified Redis SET command.
// expiration of 0 means no expiration.
func (r *Client) Set(ctx context.Context, key string, value any, expiration time.Duration) error {
	if r == nil || r.client == nil {
		return ErrClientNil
	}
	return r.client.Set(ctx, key, value, expiration).Err()
}

// Get executes a simplified Redis GET command.
// Returns redis.Nil error if key does not exist.
func (r *Client) Get(ctx context.Context, key string) (string, error) {
	if r == nil || r.client == nil {
		return "", ErrClientNil
	}
	return r.client.Get(ctx, key).Result()
}

// Del executes a simplified Redis DEL command.
// Accepts one or more keys to delete.
func (r *Client) Del(ctx context.Context, keys ...string) error {
	if r == nil || r.client == nil {
		return ErrClientNil
	}
	if len(keys) == 0 {
		return nil
	}
	return r.client.Del(ctx, keys...).Err()
}

// Exists checks if a key exists in Redis.
func (r *Client) Exists(ctx context.Context, key string) (bool, error) {
	if r == nil || r.client == nil {
		return false, ErrClientNil
	}
	n, err := r.client.Exists(ctx, key).Result()
	return n > 0, err
}

// TTL gets the remaining time-to-live of a key.
// Returns a negative duration if the key does not exist or has no expiration.
func (r *Client) TTL(ctx context.Context, key string) (time.Duration, error) {
	if r == nil || r.client == nil {
		return 0, ErrClientNil
	}
	return r.client.TTL(ctx, key).Result()
}

// SetNX executes the Redis SETNX command (Set if Not eXists).
// Returns true if the key was set, false if it already existed.
func (r *Client) SetNX(ctx context.Context, key string, value any, expiration time.Duration) (bool, error) {
	if r == nil || r.client == nil {
		return false, ErrClientNil
	}
	return r.client.SetNX(ctx, key, value, expiration).Result()
}

// SetJSON marshals a value to JSON and stores it in Redis.
func (r *Client) SetJSON(ctx context.Context, key string, value any, expiration time.Duration) error {
	if r == nil || r.client == nil {
		return ErrClientNil
	}
	bytes, err := sonic.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal json for redis: %w", err)
	}
	return r.client.Set(ctx, key, bytes, expiration).Err()
}

// GetJSON retrieves a JSON string from Redis and unmarshals it into the dest pointer.
func (r *Client) GetJSON(ctx context.Context, key string, dest any) error {
	if r == nil || r.client == nil {
		return ErrClientNil
	}
	val, err := r.client.Get(ctx, key).Bytes()
	if err != nil {
		return err // Returns redis.Nil if key does not exist
	}
	if err := sonic.Unmarshal(val, dest); err != nil {
		return fmt.Errorf("failed to unmarshal json from redis: %w", err)
	}
	return nil
}

// Metrics returns observability metrics for the Redis pool.
// Each field is a lazy function — call it to get the current value.
func (r *Client) Metrics() Metrics {
	if r == nil || r.client == nil {
		return Metrics{
			HitsTotal:     func() float64 { return 0 },
			MissesTotal:   func() float64 { return 0 },
			TimeoutsTotal: func() float64 { return 0 },
			TotalConns:    func() float64 { return 0 },
			IdleConns:     func() float64 { return 0 },
			StaleConns:    func() float64 { return 0 },
		}
	}
	return Metrics{
		HitsTotal:     func() float64 { return float64(r.client.PoolStats().Hits) },
		MissesTotal:   func() float64 { return float64(r.client.PoolStats().Misses) },
		TimeoutsTotal: func() float64 { return float64(r.client.PoolStats().Timeouts) },
		TotalConns:    func() float64 { return float64(r.client.PoolStats().TotalConns) },
		IdleConns:     func() float64 { return float64(r.client.PoolStats().IdleConns) },
		StaleConns:    func() float64 { return float64(r.client.PoolStats().StaleConns) },
	}
}

// Nil returns the redis.Nil error.
func (r *Client) Nil() redis.Error {
	return redis.Nil
}
