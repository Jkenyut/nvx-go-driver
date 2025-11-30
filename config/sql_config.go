package config

import "runtime"

// WithDefaults — versi FINAL yang 100% cocok dengan PGXClient kita
func (c SQLConfig) WithDefaults() SQLConfig {
	if !c.Enable {
		return c // kalau disable, skip semua
	}

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

	// Auto sizing berdasarkan CPU (rekomendasi production 2025)
	if c.MaxConn <= 0 {
		c.MaxConn = runtime.NumCPU() * 8 // lebih agresif dari *4 → lebih cepat di high load
	}
	if c.MinConn <= 0 {
		c.MinConn = max(4, runtime.NumCPU()) // minimal 4, biar startup cepat
	}

	// Default yang sudah terbukti optimal di production besar
	if c.MaxConnLifetime == 0 {
		c.MaxConnLifetime = 60 // 1 jam (pgx default)
	}
	if c.MaxConnIdleTime == 0 {
		c.MaxConnIdleTime = 10 // 10 menit (lebih hemat memori)
	}
	if c.HealthCheckPeriod == 0 {
		c.HealthCheckPeriod = 15 // 15 detik (cukup cepat deteksi mati)
	}
	if c.ConnectTimeout == 0 {
		c.ConnectTimeout = 10 // 10 detik (lebih aman dari 5)
	}
	if !c.AutoReconnect {
		c.AutoReconnect = true
	}

	return c
}
