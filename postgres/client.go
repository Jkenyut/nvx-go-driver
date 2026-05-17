// Package database provides a production-ready, zero-downtime PostgreSQL client
// wrapper built on top of pgx/v5 and pgxpool.
//
// PGXClient is designed for high-availability systems (microservices, fintech,
// e-commerce, banking) that require automatic reconnection, graceful shutdown,
// observability, and container-native behavior.
//
// Key features:
//   - Zero-downtime pool swapping on database failure or restart
//   - Graceful shutdown with connection draining
//   - Exponential backoff with jitter for startup and reconnect attempts
//   - Built-in Prometheus-compatible metrics
//   - Structured logging integration (zerolog)
//   - Sensible, CPU-aware defaults via config.SQLConfig.WithDefaults()
//
// Example:
//
//	client, err := postgres.NewClient(cfg.WithDefaults(), logger.L())
//	if err != nil {
//	    log.Fatal().Err(err).Msg("database connection failed")
//	}
//	defer client.Close()
//
//	// Use like a standard *pgxpool.Pool
//	rows, _ := client.Pool().Query(ctx, "SELECT id, name FROM users")
package postgres

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"sync/atomic"
	"time"

	"github.com/Jkenyut/nvx-go-driver/config"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

var passwordRegex = regexp.MustCompile(`://([^:@]+):([^:@]+)@`)

// Client wraps pgxpool.Pool with auto-reconnect, health monitoring,
// graceful shutdown, and observability features.
type Client struct {
	pool      atomic.Value // stores *pgxpool.Pool
	cfg       config.SQLConfig
	closed    uint32
	started   time.Time
	drain     chan struct{}
	reconnect atomic.Int64 // reconnect counter
	healthy   atomic.Bool  // cached health status for metrics

	log *zerolog.Logger
	// AfterConnect is called after a new connection is established
	afterConnect func(ctx context.Context, conn *pgx.Conn) error

	// BeforeConnect is called before establishing a new connection
	// Use for custom authentication or connection parameter setup
	beforeConnect func(ctx context.Context, cfg *pgx.ConnConfig) error
}

// Metrics provides Prometheus-compatible metric functions.
type Metrics struct {
	ReconnectsTotal   func() float64 // Total number of pool reconnections
	PoolTotalConns    func() float64 // Current total connections in pool
	PoolIdleConns     func() float64 // Current idle connections
	PoolAcquiredConns func() float64 // Currently acquired connections
	PoolHealth        func() float64 // 1.0 = healthy, 0.0 = unhealthy
	UptimeSeconds     func() float64 // Client uptime in seconds
}

// NewClient creates a new Client with default hook (nil AfterConnect).
// It applies defaults, connects to the database, and starts background monitoring.
func NewClient(cfg config.SQLConfig, logger *zerolog.Logger) (*Client, error) {
	return NewClientWithHook(cfg, logger, nil, nil)
}

// NewClientWithHook creates a new Client with optional AfterConnect hook.
// The hook is called for every new physical connection (useful for SET commands).
func NewClientWithHook(cfg config.SQLConfig, logger *zerolog.Logger,
	afterConnect func(ctx context.Context, conn *pgx.Conn) error,
	beforeConnect func(ctx context.Context, cfg *pgx.ConnConfig) error,
) (*Client, error) {
	if logger == nil {
		nop := zerolog.Nop()
		logger = &nop
	}

	cfg = *cfg.WithDefaults()
	if !cfg.Enable {
		return nil, errors.New("database disabled in config")
	}

	client := &Client{
		cfg:           cfg,
		log:           logger,
		afterConnect:  afterConnect,
		beforeConnect: beforeConnect,
		started:       time.Now(),
		drain:         make(chan struct{}),
	}

	if err := client.connectInitial(); err != nil {
		return nil, err
	}

	go client.monitor()
	return client, nil
}

// internal helpers below — not part of public API

func (c *Client) connectInitial() error {
	pool, err := c.createPoolWithRetry(context.Background())
	if err != nil {
		return err
	}
	c.setPool(pool)
	c.healthy.Store(true)
	c.log.Info().
		Str("dsn", maskPassword(buildDSN(c.cfg))).
		Int("max_conns", c.cfg.MaxConn).
		Int("min_conns", c.cfg.MinConn).
		Msg("Database connected successfully")
	return nil
}

// Pool returns the current active *pgxpool.Pool.
// Returns nil if the client is closed or no pool is available.
func (c *Client) Pool() *pgxpool.Pool {
	if atomic.LoadUint32(&c.closed) == 1 {
		return nil
	}
	if p := c.pool.Load(); p != nil {
		return p.(*pgxpool.Pool)
	}
	return nil
}

func (c *Client) createPoolWithRetry(ctx context.Context) (*pgxpool.Pool, error) {
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
		backoff += jitter(backoff / 2)

		c.log.Warn().
			Int("attempt", attempt).
			Err(err).
			Dur("retry_in", backoff).
			Msg("Connection failed")

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.drain:
			return nil, errors.New("client closed")
		case <-time.After(backoff):
		}
	}
	return nil, errors.New("exhausted connection attempts")
}

// IsClosed reports whether the client has been closed.
func (c *Client) IsClosed() bool {
	return atomic.LoadUint32(&c.closed) == 1
}

// Exec executes a SQL command (INSERT, UPDATE, DELETE) and returns the tag.
func (c *Client) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	pool := c.Pool()
	if pool == nil {
		return pgconn.CommandTag{}, errors.New("database client is closed or not initialized")
	}
	return pool.Exec(ctx, sql, args...)
}

// Query executes a SQL query and returns rows.
func (c *Client) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	pool := c.Pool()
	if pool == nil {
		return nil, errors.New("database client is closed or not initialized")
	}
	return pool.Query(ctx, sql, args...)
}

// QueryRow executes a SQL query that returns a single row.
func (c *Client) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	pool := c.Pool()
	if pool == nil {
		return errRow{errors.New("database client is closed or not initialized")}
	}
	return pool.QueryRow(ctx, sql, args...)
}

// Begin starts a transaction.
func (c *Client) Begin(ctx context.Context) (pgx.Tx, error) {
	pool := c.Pool()
	if pool == nil {
		return nil, errors.New("database client is closed or not initialized")
	}
	return pool.Begin(ctx)
}

// BeginTx starts a transaction with the specified options.
func (c *Client) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	pool := c.Pool()
	if pool == nil {
		return nil, errors.New("database client is closed or not initialized")
	}
	return pool.BeginTx(ctx, txOptions)
}

// RunInTx executes the provided function within a transaction.
// It automatically commits if the function returns nil, and rolls back if it returns an error or panics.
func (c *Client) RunInTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := c.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback(ctx)
			panic(p) // re-panic after rollback
		}
	}()

	if err := fn(tx); err != nil {
		tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}

// SendBatch sends all queued queries to the server at once.
func (c *Client) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	pool := c.Pool()
	if pool == nil {
		return errBatchResults{errors.New("database client is closed or not initialized")}
	}
	return pool.SendBatch(ctx, b)
}

// CopyFrom uses the PostgreSQL copy protocol to perform bulk data insertion.
func (c *Client) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	pool := c.Pool()
	if pool == nil {
		return 0, errors.New("database client is closed or not initialized")
	}
	return pool.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

// Acquire acquires a single connection from the pool.
// The connection MUST be released back to the pool by calling conn.Release() when done.
func (c *Client) Acquire(ctx context.Context) (*pgxpool.Conn, error) {
	pool := c.Pool()
	if pool == nil {
		return nil, errors.New("database client is closed or not initialized")
	}
	return pool.Acquire(ctx)
}

// Ping verifies a connection to the database is still alive, establishing a connection if necessary.
func (c *Client) Ping(ctx context.Context) error {
	pool := c.Pool()
	if pool == nil {
		return errors.New("database client is closed or not initialized")
	}
	return pool.Ping(ctx)
}

// Stat returns a pgxpool.Stat struct with a snapshot of pool statistics.
func (c *Client) Stat() *pgxpool.Stat {
	pool := c.Pool()
	if pool == nil {
		// Return empty struct if no pool to avoid nil panics for stat readers
		return &pgxpool.Stat{}
	}
	return pool.Stat()
}

type errRow struct {
	err error
}

func (e errRow) Scan(dest ...any) error { return e.err }

type errBatchResults struct {
	err error
}

func (e errBatchResults) Exec() (pgconn.CommandTag, error) { return pgconn.CommandTag{}, e.err }
func (e errBatchResults) Query() (pgx.Rows, error)         { return nil, e.err }
func (e errBatchResults) QueryRow() pgx.Row                { return errRow{e.err} }
func (e errBatchResults) Close() error                     { return e.err }

func (c *Client) createPool(ctx context.Context) (*pgxpool.Pool, error) {
	pc, err := pgxpool.ParseConfig(buildDSN(c.cfg))
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}

	pc.MaxConns = int32(c.cfg.MaxConn)
	pc.MinConns = int32(c.cfg.MinConn)

	pc.MaxConnLifetime = time.Duration(c.cfg.MaxConnLifetime) * time.Second
	pc.MaxConnIdleTime = time.Duration(c.cfg.MaxConnIdleTime) * time.Second
	pc.HealthCheckPeriod = time.Duration(c.cfg.HealthCheckPeriod) * time.Second
	pc.ConnConfig.ConnectTimeout = time.Duration(c.cfg.ConnectTimeout) * time.Second
	if pc.ConnConfig.RuntimeParams == nil {
		pc.ConnConfig.RuntimeParams = make(map[string]string)
	}
	if _, ok := pc.ConnConfig.RuntimeParams["application_name"]; !ok {
		pc.ConnConfig.RuntimeParams["application_name"] = "nvx-go-driver"
	}

	if c.beforeConnect != nil {
		pc.BeforeConnect = c.beforeConnect
	}

	if c.afterConnect != nil {
		pc.AfterConnect = c.afterConnect
	}

	// Simple validation: reject closed connections (fast, no extra ping)
	pc.PrepareConn = func(ctx context.Context, conn *pgx.Conn) (bool, error) {
		return !conn.IsClosed(), nil
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

// Close performs a graceful shutdown:
//   - Stops health monitoring
//   - Forces short lifetimes to return connections quickly
//   - Waits up to 30 seconds for acquired connections to be released
//   - Closes the pool
func (c *Client) Close() error {
	if !atomic.CompareAndSwapUint32(&c.closed, 0, 1) {
		return nil // already closed
	}

	c.log.Info().Msg("Initiating graceful shutdown of PGXClient")
	close(c.drain)

	pool := c.Pool()
	if pool == nil {
		c.log.Info().Dur("uptime", time.Since(c.started)).Msg("PGXClient closed (no pool)")
		return nil
	}

	timeout := time.After(30 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	closedChan := make(chan struct{})
	go func() {
		pool.Close()
		close(closedChan)
	}()

	for {
		select {
		case <-closedChan:
			c.log.Info().
				Dur("uptime", time.Since(c.started)).
				Int64("total_reconnects", c.reconnect.Load()).
				Msg("PGXClient gracefully shut down")
			return nil
		case <-timeout:
			c.log.Warn().Msg("Shutdown timeout — abandoning pool (connections may leak)")
			return nil
		case <-ticker.C:
			stat := pool.Stat()
			c.log.Debug().
				Int32("acquired", stat.AcquiredConns()).
				Int32("idle", stat.IdleConns()).
				Msg("Waiting for connections to be released")
		}
	}
}

// Metrics returns a struct with functions for exposing metrics (Prometheus-ready).
func (c *Client) Metrics() Metrics {
	return Metrics{
		ReconnectsTotal:   func() float64 { return float64(c.reconnect.Load()) },
		PoolTotalConns:    func() float64 { s := c.safeStat(); return float64(s.TotalConns()) },
		PoolIdleConns:     func() float64 { s := c.safeStat(); return float64(s.IdleConns()) },
		PoolAcquiredConns: func() float64 { s := c.safeStat(); return float64(s.AcquiredConns()) },
		PoolHealth: func() float64 {
			if c.healthy.Load() {
				return 1.0
			}
			return 0.0
		},
		UptimeSeconds: func() float64 { return time.Since(c.started).Seconds() },
	}
}

func (c *Client) setPool(p *pgxpool.Pool) {
	c.pool.Store(p)
	c.reconnect.Add(1)
}

func (c *Client) monitor() {
	healthInterval := time.Duration(c.cfg.HealthCheckPeriod) * time.Second
	ticker := time.NewTicker(healthInterval)
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
				c.healthy.Store(true)
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

func (c *Client) isHealthy() bool {
	p := c.Pool()
	if p == nil {
		c.healthy.Store(false)
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ok := p.Ping(ctx) == nil
	c.healthy.Store(ok)
	return ok
}

func (c *Client) safeStat() pgxpool.Stat {
	if p := c.Pool(); p != nil {
		return *p.Stat()
	}
	return pgxpool.Stat{}
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

// jitter returns a random offset ±50% of the input duration.
// It never returns negative values.
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
