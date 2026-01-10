package kafka

import (
	"testing"

	"github.com/Jkenyut/nvx-go-driver/config"
	"github.com/rs/zerolog"
)

func TestApplyDefaults(t *testing.T) {
	tests := []struct {
		name     string
		input    config.KafkaConfig
		expected string // expected SecurityProtocol
	}{
		{
			name:     "Auth implied SASL_SSL",
			input:    config.KafkaConfig{Username: "user", Password: "pwd"},
			expected: "SASL_SSL",
		},
		{
			name:     "No Auth implied PLAINTEXT",
			input:    config.KafkaConfig{},
			expected: "PLAINTEXT",
		},
		{
			name: "Explicit Override",
			input: config.KafkaConfig{
				Username:         "user",
				SecurityProtocol: "SASL_PLAINTEXT",
			},
			expected: "SASL_PLAINTEXT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := applyDefaults(tt.input)
			if got.SecurityProtocol != tt.expected {
				t.Errorf("SecurityProtocol = %v, want %v", got.SecurityProtocol, tt.expected)
			}
			// Check host default
			if got.Host == "" {
				t.Error("Host should have a default value")
			}
		})
	}
}

func TestNewKafkaClient_Disabled(t *testing.T) {
	logger := zerolog.Nop()
	cfg := config.KafkaConfig{Enable: false}

	client, err := NewKafkaClient(cfg, &logger)
	if err == nil {
		t.Error("expected error when disabled, got nil")
	}
	if client != nil {
		t.Error("expected nil client when disabled")
	}
}
