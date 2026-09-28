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
				ReconnectDuration: 5000,
				ConnectTimeout:    10000,
				PublishTimeout:    5000,
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
				ReconnectDuration: 5000,
				ConnectTimeout:    10000,
				PublishTimeout:    5000,
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

	client, err := NewClient(&cfg, WithLogger(nil))
	if err == nil {
		t.Error("expected error when disabled, got nil")
	}
	if client != nil {
		t.Error("expected nil client when disabled")
	}
}

func TestNewConsumer_Options(t *testing.T) {
	consumer := NewConsumer(nil, "my-queue",
		WithConsumerQos(10),
		WithAutoAck(true),
		WithExclusive(true),
		WithNoLocal(true),
		WithNoWait(true),
	)

	if consumer.queue != "my-queue" {
		t.Errorf("queue = %v, want my-queue", consumer.queue)
	}
	if consumer.qos != 10 {
		t.Errorf("qos = %v, want 10", consumer.qos)
	}
	if !consumer.autoAck {
		t.Errorf("autoAck = false, want true")
	}
	if !consumer.exclusive {
		t.Errorf("exclusive = false, want true")
	}
	if !consumer.noLocal {
		t.Errorf("noLocal = false, want true")
	}
	if !consumer.noWait {
		t.Errorf("noWait = false, want true")
	}
}

func TestMaskURL(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{
			input:    "amqp://guest:secret123@localhost:5672/",
			expected: "amqp://guest:****@localhost:5672/",
		},
		{
			input:    "amqps://admin:super_secret_pw@rabbitmq.prod:5671/vhost",
			expected: "amqps://admin:****@rabbitmq.prod:5671/vhost",
		},
	}

	for _, tt := range tests {
		got := maskURL(tt.input)
		if got != tt.expected {
			t.Errorf("maskURL(%q) = %q, want %q", tt.input, got, tt.expected)
		}
	}
}

func TestConsumer_SettersGuardWhenStarted(t *testing.T) {
	consumer := NewConsumer(nil, "queue", WithConsumerQos(5))
	consumer.started.Store(true) // simulate active consumer

	consumer.SetQos(99)
	consumer.SetAutoAck(true)
	consumer.SetExclusive(true)
	consumer.SetNoLocal(true)
	consumer.SetNoWait(true)

	if consumer.qos != 5 {
		t.Errorf("qos was modified after start: got %d, want 5", consumer.qos)
	}
	if consumer.autoAck != false {
		t.Errorf("autoAck was modified after start")
	}
	if consumer.exclusive != false {
		t.Errorf("exclusive was modified after start")
	}
}
