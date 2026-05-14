package redis

import (
	"context"
	"encoding/json"
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
	HitsTotal     float64
	MissesTotal   float64
	TimeoutsTotal float64
	TotalConns    float64
	IdleConns     float64
	StaleConns    float64
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
		DB:           cfg.Database,
		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.MinIdleConn,
		MaxIdleConns: cfg.MaxIdleConn,
		DialTimeout:  time.Duration(cfg.ConnectTimeout) * time.Second,
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
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = 5
	}
	return cfg
}

// Client returns the underlying *redis.Client for direct usage.
func (r *Client) Client() *redis.Client {
	if r.client == nil {
		return nil
	}
	return r.client
}

// Close gracefully closes the Redis client connection.
func (r *Client) Close() error {
	if r.client == nil {
		return errors.New("redis client is nil")
	}
	r.log.Info().Msg("Closing Redis client")
	return r.client.Close()
}

// Ping verifies the connection to Redis is alive.
// Useful for application health check endpoints.
func (r *Client) Ping(ctx context.Context) error {
	if r.client == nil {
		return errors.New("redis client is nil")
	}
	return r.client.Ping(ctx).Err()
}

// Set executes a simplified Redis SET command.
// expiration of 0 means no expiration.
func (r *Client) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	if r.client == nil {
		return errors.New("redis client is nil")
	}
	return r.client.Set(ctx, key, value, expiration).Err()
}

// Get executes a simplified Redis GET command.
// Returns redis.Nil error if key does not exist.
func (r *Client) Get(ctx context.Context, key string) (string, error) {
	if r.client == nil {
		return "", errors.New("redis client is nil")
	}
	return r.client.Get(ctx, key).Result()
}

// Del executes a simplified Redis DEL command.
func (r *Client) Del(ctx context.Context, key string) error {
	if r.client == nil {
		return errors.New("redis client is nil")
	}
	return r.client.Del(ctx, key).Err()
}

// Exists checks if a key exists in Redis.
func (r *Client) Exists(ctx context.Context, key string) (bool, error) {
	if r.client == nil {
		return false, errors.New("redis client is nil")
	}
	n, err := r.client.Exists(ctx, key).Result()
	return n > 0, err
}

// TTL gets the remaining time-to-live of a key.
// Returns a negative duration if the key does not exist or has no expiration.
func (r *Client) TTL(ctx context.Context, key string) (time.Duration, error) {
	if r.client == nil {
		return 0, errors.New("redis client is nil")
	}
	return r.client.TTL(ctx, key).Result()
}

// SetNX executes the Redis SETNX command (Set if Not eXists).
// Returns true if the key was set, false if it already existed.
func (r *Client) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) (bool, error) {
	if r.client == nil {
		return false, errors.New("redis client is nil")
	}
	return r.client.SetNX(ctx, key, value, expiration).Result()
}

// SetJSON marshals a value to JSON and stores it in Redis.
func (r *Client) SetJSON(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	if r.client == nil {
		return errors.New("redis client is nil")
	}
	bytes, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal json for redis: %w", err)
	}
	return r.client.Set(ctx, key, bytes, expiration).Err()
}

// GetJSON retrieves a JSON string from Redis and unmarshals it into the dest pointer.
func (r *Client) GetJSON(ctx context.Context, key string, dest interface{}) error {
	if r.client == nil {
		return errors.New("redis client is nil")
	}
	val, err := r.client.Get(ctx, key).Bytes()
	if err != nil {
		return err // Returns redis.Nil if key does not exist
	}
	if err := json.Unmarshal(val, dest); err != nil {
		return fmt.Errorf("failed to unmarshal json from redis: %w", err)
	}
	return nil
}

// Metrics returns observability metrics for the Redis pool.
func (r *Client) Metrics() Metrics {
	if r.client == nil {
		return Metrics{}
	}
	stats := r.client.PoolStats()
	return Metrics{
		HitsTotal:     float64(stats.Hits),
		MissesTotal:   float64(stats.Misses),
		TimeoutsTotal: float64(stats.Timeouts),
		TotalConns:    float64(stats.TotalConns),
		IdleConns:     float64(stats.IdleConns),
		StaleConns:    float64(stats.StaleConns),
	}
}

func (r *Client) Nil() redis.Error {
	return redis.Nil
}
