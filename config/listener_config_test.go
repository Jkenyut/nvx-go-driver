package config

import "testing"

func TestListener_Environment(t *testing.T) {
	tests := []struct {
		name     string
		input    Listener
		expected string
	}{
		{"empty env defaults to development", Listener{}, "development"},
		{"production env", Listener{Env: "production"}, "production"},
		{"staging env", Listener{Env: "staging"}, "staging"},
		{"custom env", Listener{Env: "custom"}, "custom"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.input.Environment()
			if got != tt.expected {
				t.Errorf("Environment() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestListener_ServiceName(t *testing.T) {
	tests := []struct {
		name     string
		input    Listener
		expected string
	}{
		{"empty name defaults to unknown-service", Listener{}, "unknown-service"},
		{"with name", Listener{NameService: "my-service"}, "my-service"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.input.ServiceName()
			if got != tt.expected {
				t.Errorf("ServiceName() = %q, want %q", got, tt.expected)
			}
		})
	}
}
