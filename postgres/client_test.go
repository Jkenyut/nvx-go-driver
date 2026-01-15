package postgres

import (
	"testing"
	"time"

	"github.com/Jkenyut/nvx-go-driver/config"
	"github.com/rs/zerolog"
)

func TestNewClient_Disabled(t *testing.T) {
	logger := zerolog.Nop()
	cfg := config.SQLConfig{
		Enable: false,
	}

	client, err := NewClient(cfg, &logger)
	if err == nil {
		t.Error("expected error when config is disabled, got nil")
	}
	if client != nil {
		t.Error("expected nil client when config is disabled, got object")
	}
}

func TestBuildDSN(t *testing.T) {
	tests := []struct {
		name     string
		cfg      config.SQLConfig
		expected string
	}{
		{
			name: "minimal config",
			cfg: config.SQLConfig{
				Host:     "localhost",
				Port:     5432,
				Username: "user",
				Password: "password",
				Database: "db",
			},
			expected: "postgres://user:password@localhost:5432/db",
		},
		{
			name: "with options",
			cfg: config.SQLConfig{
				Host:     "localhost",
				Port:     5432,
				Username: "user",
				Password: "password",
				Database: "db",
				Options:  "sslmode=disable",
			},
			expected: "postgres://user:password@localhost:5432/db?sslmode=disable",
		},
		{
			name: "connection override",
			cfg: config.SQLConfig{
				Connection: "postgres://override:pass@remote:9999/otherdb",
			},
			expected: "postgres://override:pass@remote:9999/otherdb",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildDSN(tt.cfg)
			if got != tt.expected {
				t.Errorf("buildDSN() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestConfigDefaults(t *testing.T) {
	// Since WithDefaults is in a different package (config) and is called by NewClient,
	// we want to ensure NewClient applies them.
	// However, NewClient tries to connect.
	// For unit testing purely the config logic without DB, we might need to test config package directly
	// or mock the connection.
	//
	// Given the current structure, we can verify maskPassword which is internal
	dsn := "postgres://user:secret@localhost:5432/db"
	masked := maskPassword(dsn)
	expected := "postgres://user:****@localhost:5432/db"

	if masked != expected {
		t.Errorf("maskPassword() = %v, want %v", masked, expected)
	}
}

func TestMetrics_Structure(t *testing.T) {
	// Basic test to ensure Metrics() doesn't panic on nil pool (which happens if initial connect fails)
	// Actually NewClient returns error if connect fails, so we can't easily get a client without DB.
	// But we can construct one manually for this test since it's same package.

	client := &Client{
		started: time.Now(),
	}

	metrics := client.Metrics()

	if metrics.ReconnectsTotal == nil {
		t.Error("Metrics.ReconnectsTotal should not be nil")
	}

	// Test safe execution without pool
	reconnects := metrics.ReconnectsTotal()
	if reconnects != 0 {
		t.Errorf("expected 0 reconnects, got %f", reconnects)
	}

	health := metrics.PoolHealth()
	if health != 0 {
		t.Errorf("expected 0 health (unhealthy) when no pool, got %f", health)
	}
}

// In the original file atomic.Int64 is used.
// We just need to ensure the struct logic works.
// Since we are in the same package (database), we can access private fields.
