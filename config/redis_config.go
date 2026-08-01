package config

// WithDefaults applies sensible defaults for go-redis/v9 client configuration.
// All time-based values are in **seconds** to match time.Duration usage.
func (c *RedisConfig) WithDefaults() *RedisConfig {
	if c.Host == "" {
		c.Host = "127.0.0.1"
	}
	if c.Port == 0 {
		c.Port = 6379
	}
	if c.PoolSize == 0 {
		c.PoolSize = 10
	}
	if c.MinIdleConn == 0 {
		c.MinIdleConn = 5
	}
	if c.MaxIdleConn == 0 {
		c.MaxIdleConn = 15
	}
	if c.PoolTimeout == 0 {
		c.PoolTimeout = 30
	}
	if c.ConnectTimeout == 0 {
		c.ConnectTimeout = 5
	}
	if c.ConnMaxLife == 0 {
		c.ConnMaxLife = 600
	}
	if c.StartInterval == 0 {
		c.StartInterval = 2
	}
	if c.MaxError == 0 {
		c.MaxError = 5
	}
	return c
}
