// Package config provides configuration structs.
package config

import (
	"runtime"
)

// WithDefaults applies sensible, battle-tested defaults for pgx/v5 pool
// All time-based values are in **seconds** to match time.Duration usage
func (c *SQLConfig) WithDefaults() *SQLConfig {
	if !c.Enable {
		return c // disabled → no changes
	}

	// Connection basics
	if c.Host == "" {
		c.Host = "0.0.0.0"
	}
	if c.Port == 0 {
		c.Port = 5432
	}
	if c.Username == "" {
		c.Username = "postgres"
	}
	if c.Password == "" {
		c.Password = "postgres"
	}
	if c.Database == "" {
		c.Database = "postgres"
	}
	if c.Schema == "" {
		c.Schema = "public"
	}
	if c.Options == "" {
		c.Options = "sslmode=disable"
	}

	// Auto-scaling connection pool based on CPU cores (2025 best practice)
	if c.MaxConn <= 0 {
		c.MaxConn = runtime.NumCPU() * 8 // aggressive for high concurrency
	}
	if c.MinConn <= 0 {
		c.MinConn = max(4, runtime.NumCPU()) // at least 4 for fast startup
	}

	// Time-based defaults in **seconds**
	if c.MaxConnLifetime == 0 {
		c.MaxConnLifetime = 3600 // 1 hour — prevents stale connections
	}
	if c.MaxConnIdleTime == 0 {
		c.MaxConnIdleTime = 600 // 10 minutes — frees unused memory
	}
	if c.HealthCheckPeriod == 0 {
		c.HealthCheckPeriod = 15 // 15 seconds — fast failure detection
	}
	if c.ConnectTimeout == 0 {
		c.ConnectTimeout = 10 // 10 seconds — safe for cloud/network flakes
	}

	return c
}
