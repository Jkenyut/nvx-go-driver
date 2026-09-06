package rabbitmq

import (
	"testing"

	"github.com/Jkenyut/nvx-go-driver/config"
)

func TestWithDefaults(t *testing.T) {
	tests := []struct {
		name     string
		input    config.RabbitMQConfig
		expected config.RabbitMQConfig
	}{
		{
			name:  "Empty Config",
			input: config.RabbitMQConfig{},
			expected: config.RabbitMQConfig{
				Host:              "127.0.0.1",
				Port:              5672,
				Username:          "guest",
				Password:          "guest",
				ReconnectDuration: 5,
				ConnectTimeout:    10,
				PublishTimeout:    5,
			},
		},
		{
			name: "Partial Config",
			input: config.RabbitMQConfig{
				Host: "rabbit-prod",
			},
			expected: config.RabbitMQConfig{
				Host:              "rabbit-prod",
				Port:              5672,
				Username:          "guest",
				Password:          "guest",
				ReconnectDuration: 5,
				ConnectTimeout:    10,
				PublishTimeout:    5,
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
			if got.Username != tt.expected.Username {
				t.Errorf("Username = %v, want %v", got.Username, tt.expected.Username)
			}
			if got.ReconnectDuration != tt.expected.ReconnectDuration {
				t.Errorf("ReconnectDuration = %v, want %v", got.ReconnectDuration, tt.expected.ReconnectDuration)
			}
			if got.ConnectTimeout != tt.expected.ConnectTimeout {
				t.Errorf("ConnectTimeout = %v, want %v", got.ConnectTimeout, tt.expected.ConnectTimeout)
			}
			if got.PublishTimeout != tt.expected.PublishTimeout {
				t.Errorf("PublishTimeout = %v, want %v", got.PublishTimeout, tt.expected.PublishTimeout)
			}
		})
	}
}

func TestNewClient_Disabled(t *testing.T) {
	cfg := config.RabbitMQConfig{Enable: false}

	client, err := NewClient(&cfg, nil)
	if err == nil {
		t.Error("expected error when disabled, got nil")
	}
	if client != nil {
		t.Error("expected nil client when disabled")
	}
}
