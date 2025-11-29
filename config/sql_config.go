package config

import "runtime"

func (c SQLConfig) WithDefaults() SQLConfig {
	if c.Host == "" {
		c.Host = "127.0.0.1"
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
	if c.Options == "" {
		c.Options = "sslmode=disable"
	}

	if c.MaxConn == 0 {
		c.MaxConn = runtime.NumCPU() * 4
	}
	if c.MinConn == 0 {
		c.MinConn = runtime.NumCPU()
	}
	if c.MaxConnIdleTime == 0 {
		c.MaxConnIdleTime = 60
	}
	if c.LifeTime == 0 {
		c.LifeTime = 30
	}
	if c.HealthCheckPeriod == 0 {
		c.HealthCheckPeriod = 30
	}
	if c.ConnectTimeout == 0 {
		c.ConnectTimeout = 5
	}
	if !c.AutoReconnect {
		c.AutoReconnect = true
	}

	return c
}
