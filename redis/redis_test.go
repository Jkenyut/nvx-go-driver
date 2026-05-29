package redis

import (
	"testing"

	"github.com/Jkenyut/nvx-go-driver/config"
	"github.com/rs/zerolog"
)

func TestWithDefaults(t *testing.T) {
	tests := []struct {
		name     string
		input    config.RedisConfig
		expected config.RedisConfig
	}{
		{
			name:  "Empty Config",
			input: config.RedisConfig{},
			expected: config.RedisConfig{
				Host:        "127.0.0.1",
				Port:        6379,
				PoolSize:    10,
				MinIdleConn: 5,
				PoolTimeout: 30,
			},
		},
		{
			name: "Partial Config",
			input: config.RedisConfig{
				Host:     "redis-prod",
				PoolSize: 50,
			},
			expected: config.RedisConfig{
				Host:        "redis-prod",
				Port:        6379,
				PoolSize:    50,
				MinIdleConn: 5,
				PoolTimeout: 30,
			},
		},
		{
			name: "Full Config",
			input: config.RedisConfig{
				Host:        "custom",
				Port:        1234,
				PoolSize:    100,
				MinIdleConn: 20,
				PoolTimeout: 60,
			},
			expected: config.RedisConfig{
				Host:        "custom",
				Port:        1234,
				PoolSize:    100,
				MinIdleConn: 20,
				PoolTimeout: 60,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := tt.input
			got := input.WithDefaults()
			if got.Host != tt.expected.Host {
				t.Errorf("Host = %v, want %v", got.Host, tt.expected.Host)
			}
			if got.Port != tt.expected.Port {
				t.Errorf("Port = %v, want %v", got.Port, tt.expected.Port)
			}
			if got.PoolSize != tt.expected.PoolSize {
				t.Errorf("PoolSize = %v, want %v", got.PoolSize, tt.expected.PoolSize)
			}
			if got.MinIdleConn != tt.expected.MinIdleConn {
				t.Errorf("MinIdleConn = %v, want %v", got.MinIdleConn, tt.expected.MinIdleConn)
			}
			if got.PoolTimeout != tt.expected.PoolTimeout {
				t.Errorf("PoolTimeout = %v, want %v", got.PoolTimeout, tt.expected.PoolTimeout)
			}
		})
	}
}

func TestNewClient_Disabled(t *testing.T) {
	logger := zerolog.Nop()
	cfg := config.RedisConfig{Enable: false}

	client, err := NewClient(&cfg, &logger)
	if err == nil {
		t.Error("expected error when disabled, got nil")
	}
	if client != nil {
		t.Error("expected nil client when disabled")
	}
}
