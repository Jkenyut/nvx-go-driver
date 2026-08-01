package config

import (
	"runtime"
	"testing"
)

func TestSQLConfig_WithDefaults(t *testing.T) {
	tests := []struct {
		name     string
		input    SQLConfig
		validate func(t *testing.T, cfg *SQLConfig)
	}{
		{
			name:  "Disabled config returns unchanged",
			input: SQLConfig{Enable: false},
			validate: func(t *testing.T, cfg *SQLConfig) {
				if cfg.Host != "" {
					t.Errorf("expected empty host for disabled config, got %q", cfg.Host)
				}
			},
		},
		{
			name:  "Empty enabled config gets defaults",
			input: SQLConfig{Enable: true},
			validate: func(t *testing.T, cfg *SQLConfig) {
				if cfg.Host != "0.0.0.0" {
					t.Errorf("Host = %q, want %q", cfg.Host, "0.0.0.0")
				}
				if cfg.Port != 5432 {
					t.Errorf("Port = %d, want %d", cfg.Port, 5432)
				}
				if cfg.Username != "postgres" {
					t.Errorf("Username = %q, want %q", cfg.Username, "postgres")
				}
				expectedMaxConn := runtime.NumCPU() * 8
				if cfg.MaxConn != expectedMaxConn {
					t.Errorf("MaxConn = %d, want %d (NumCPU*8)", cfg.MaxConn, expectedMaxConn)
				}
				expectedMinConn := max(4, runtime.NumCPU())
				if cfg.MinConn != expectedMinConn {
					t.Errorf("MinConn = %d, want %d", cfg.MinConn, expectedMinConn)
				}
				if cfg.MaxConnLifetime != 3600 {
					t.Errorf("MaxConnLifetime = %d, want 3600", cfg.MaxConnLifetime)
				}
				if cfg.MaxConnIdleTime != 600 {
					t.Errorf("MaxConnIdleTime = %d, want 600", cfg.MaxConnIdleTime)
				}
				if cfg.HealthCheckPeriod != 15 {
					t.Errorf("HealthCheckPeriod = %d, want 15", cfg.HealthCheckPeriod)
				}
				if cfg.ConnectTimeout != 10 {
					t.Errorf("ConnectTimeout = %d, want 10", cfg.ConnectTimeout)
				}
			},
		},
		{
			name: "Preserves user values",
			input: SQLConfig{
				Enable:   true,
				Host:     "db.example.com",
				Port:     5433,
				Username: "admin",
				MaxConn:  50,
				MinConn:  10,
			},
			validate: func(t *testing.T, cfg *SQLConfig) {
				if cfg.Host != "db.example.com" {
					t.Errorf("Host = %q, want %q", cfg.Host, "db.example.com")
				}
				if cfg.Port != 5433 {
					t.Errorf("Port = %d, want %d", cfg.Port, 5433)
				}
				if cfg.Username != "admin" {
					t.Errorf("Username = %q, want %q", cfg.Username, "admin")
				}
				if cfg.MaxConn != 50 {
					t.Errorf("MaxConn = %d, want %d", cfg.MaxConn, 50)
				}
				if cfg.MinConn != 10 {
					t.Errorf("MinConn = %d, want %d", cfg.MinConn, 10)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.input.WithDefaults()
			tt.validate(t, result)
		})
	}
}
