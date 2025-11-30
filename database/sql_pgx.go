// database/pgx_client.go
package database

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/Jkenyut/nvx-go-driver/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

var passwordRegex = regexp.MustCompile(`://([^:@]+):([^:@]+)@`)

type PGXClient struct {
	pool      atomic.Value // *pgxpool.Pool
	cfg       config.SQLConfig
	closed    uint32
	started   time.Time
	drain     chan struct{}
	reconnect atomic.Int64

	log          *zerolog.Logger
	AfterConnect func(ctx context.Context, conn *pgx.Conn) error
}

type PGXMetrics struct {
	ReconnectsTotal   func() float64
	PoolTotalConns    func() float64
	PoolIdleConns     func() float64
	PoolAcquiredConns func() float64
	PoolHealth        func() float64
	UptimeSeconds     func() float64
}

func NewPGXClient(cfg config.SQLConfig, logger *zerolog.Logger) (*PGXClient, error) {
	return NewPGXClientWithHook(cfg, logger, nil)
}

func NewPGXClientWithHook(cfg config.SQLConfig, logger *zerolog.Logger, hook func(context.Context, *pgx.Conn) error) (*PGXClient, error) {
	if logger == nil {
		nop := zerolog.Nop()
		logger = &nop
	}

	cfg = cfg.WithDefaults()
	if !cfg.Enable {
		return nil, errors.New("database disabled in config")
	}

	if cfg.MaxConn == 0 {
		cfg.MaxConn = runtime.NumCPU() * 8
	}
	if cfg.MinConn == 0 {
		cfg.MinConn = max(4, runtime.NumCPU())
	}
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = 10
	}
	if cfg.HealthCheckPeriod == 0 {
		cfg.HealthCheckPeriod = 15
	}

	client := &PGXClient{
		cfg:          cfg,
		log:          logger,
		AfterConnect: hook,
		started:      time.Now(),
		drain:        make(chan struct{}),
	}

	if err := client.connectInitial(); err != nil {
		return nil, err
	}

	go client.monitor()
	return client, nil
}

func (c *PGXClient) connectInitial() error {
	pool, err := c.createPoolWithRetry(context.Background())
	if err != nil {
		return err
	}
	c.setPool(pool)
	c.log.Info().
		Str("dsn", maskPassword(buildDSN(c.cfg))).
		Int("max_conns", c.cfg.MaxConn).
		Int("min_conns", c.cfg.MinConn).
		Msg("Database connected successfully")
	return nil
}

func (c *PGXClient) Pool() *pgxpool.Pool {
	if atomic.LoadUint32(&c.closed) == 1 {
		return nil
	}
	if p := c.pool.Load(); p != nil {
		return p.(*pgxpool.Pool)
	}
	return nil
}

func (c *PGXClient) setPool(p *pgxpool.Pool) {
	c.pool.Store(p)
	c.reconnect.Add(1)
}

func (c *PGXClient) IsClosed() bool {
	return atomic.LoadUint32(&c.closed) == 1
}

func (c *PGXClient) Close() error {
	if !atomic.CompareAndSwapUint32(&c.closed, 0, 1) {
		return nil
	}

	c.log.Info().Msg("Initiating graceful shutdown of PGXClient")
	close(c.drain)

	pool := c.Pool()
	if pool == nil {
		c.log.Info().Dur("uptime", time.Since(c.started)).Msg("PGXClient closed (no pool)")
		return nil
	}

	pool.Config().MaxConnLifetime = 3 * time.Second
	pool.Config().MaxConnIdleTime = 2 * time.Second

	timeout := time.After(30 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			c.log.Warn().Msg("Shutdown timeout — forcing close")
			pool.Close()
			return nil
		case <-ticker.C:
			stat := pool.Stat()
			if stat.AcquiredConns() == 0 {
				pool.Close()
				c.log.Info().
					Dur("uptime", time.Since(c.started)).
					Int64("total_reconnects", c.reconnect.Load()).
					Msg("PGXClient gracefully shut down")
				return nil
			}
			c.log.Debug().
				Int32("acquired", stat.AcquiredConns()).
				Int32("idle", stat.IdleConns()).
				Msg("Waiting for connections to be released")
		}
	}
}

func (c *PGXClient) createPool(ctx context.Context) (*pgxpool.Pool, error) {
	pc, err := pgxpool.ParseConfig(buildDSN(c.cfg))
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}

	pc.MaxConns = int32(c.cfg.MaxConn)
	pc.MinConns = int32(c.cfg.MinConn)
	pc.MaxConnLifetime = 60 * time.Minute
	pc.MaxConnIdleTime = 10 * time.Minute
	pc.HealthCheckPeriod = 15 * time.Second
	pc.ConnConfig.ConnectTimeout = time.Duration(c.cfg.ConnectTimeout) * time.Second
	pc.ConnConfig.RuntimeParams = map[string]string{"application_name": "nvx-go-driver"}

	if c.AfterConnect != nil {
		pc.AfterConnect = c.AfterConnect
	}

	// JANGAN PING DI SINI — lambat & berbahaya
	pc.PrepareConn = func(ctx context.Context, conn *pgx.Conn) (bool, error) {
		if conn == nil || conn.IsClosed() {
			return false, nil
		}
		return true, nil // Cukup! Pool health check sudah handle
	}

	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	if errs := pool.Ping(ctx); errs != nil {
		pool.Close()
		return nil, fmt.Errorf("initial ping failed: %w", errs)
	}

	return pool, nil
}

func (c *PGXClient) createPoolWithRetry(ctx context.Context) (*pgxpool.Pool, error) {
	baseDelay := time.Second
	for attempt := 1; attempt <= 20; attempt++ {
		if c.IsClosed() {
			return nil, errors.New("client closed")
		}

		pctx, cancel := context.WithTimeout(ctx, 12*time.Second)
		pool, err := c.createPool(pctx)
		cancel()
		if err == nil {
			c.log.Info().Int("attempt", attempt).Msg("Database connected")
			return pool, nil
		}

		backoff := time.Duration(math.Pow(1.8, float64(attempt-1))) * baseDelay
		if backoff > 30*time.Second {
			backoff = 30 * time.Second
		}
		backoff += jitter(backoff / 2) // jitter ±50%

		c.log.Warn().
			Int("attempt", attempt).
			Err(err).
			Dur("retry_in", backoff).
			Msg("Connection failed")

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
	}
	return nil, errors.New("exhausted connection attempts")
}

func (c *PGXClient) monitor() {
	ticker := time.NewTicker(time.Duration(c.cfg.HealthCheckPeriod) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.drain:
			return
		case <-ticker.C:
			if c.IsClosed() || c.isHealthy() {
				continue
			}

			c.log.Warn().Msg("Pool unhealthy → reconnecting")
			if newPool, err := c.createPoolWithRetry(context.Background()); err == nil {
				old := c.Pool()
				c.setPool(newPool)
				if old != nil {
					go func(p *pgxpool.Pool) {
						time.Sleep(5 * time.Second)
						p.Close()
						c.log.Info().Msg("Old pool closed")
					}(old)
				}
				c.log.Info().Int64("reconnects", c.reconnect.Load()).Msg("Pool swapped")
			}
		}
	}
}

func (c *PGXClient) isHealthy() bool {
	p := c.Pool()
	if p == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return p.Ping(ctx) == nil
}

func (c *PGXClient) safeStat() pgxpool.Stat {
	if p := c.Pool(); p != nil {
		return *p.Stat()
	}
	return pgxpool.Stat{}
}

func (c *PGXClient) Metrics() PGXMetrics {
	s := c.safeStat()
	return PGXMetrics{
		ReconnectsTotal:   func() float64 { return float64(c.reconnect.Load()) },
		PoolTotalConns:    func() float64 { return float64(s.TotalConns()) },
		PoolIdleConns:     func() float64 { return float64(s.IdleConns()) },
		PoolAcquiredConns: func() float64 { return float64(s.AcquiredConns()) },
		PoolHealth: func() float64 {
			if c.isHealthy() {
				return 1.0
			}
			return 0.0
		},
		UptimeSeconds: func() float64 { return time.Since(c.started).Seconds() },
	}
}

func buildDSN(cfg config.SQLConfig) string {
	if cfg.Connection != "" {
		return cfg.Connection
	}
	base := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.Database)
	if cfg.Options != "" {
		return base + "?" + cfg.Options
	}
	return base
}

func maskPassword(dsn string) string {
	return passwordRegex.ReplaceAllString(dsn, "://$1:****@")
}

// jitter ±50% dari duration
func jitter(d time.Duration) time.Duration {
	if d <= 100*time.Millisecond {
		return 0
	}
	maxJitter := int64(d / 2)
	if maxJitter == 0 {
		return 0
	}
	j, _ := rand.Int(rand.Reader, big.NewInt(maxJitter*2))
	offset := time.Duration(j.Int64() - maxJitter)
	return offset
}
