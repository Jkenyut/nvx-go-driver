package config

import "testing"

func TestListener_Environment(t *testing.T) {
	tests := []struct {
		name     string
		input    Listener
		expected string
	}{
		{"empty env defaults to development", Listener{}, "development"},
		{"whitespace env defaults to development", Listener{Env: "   "}, "development"},
		{"production env lowercase", Listener{Env: "production"}, "production"},
		{"production env uppercase", Listener{Env: "PRODUCTION"}, "production"},
		{"production env mixed case", Listener{Env: "Production"}, "production"},
		{"prod alias uppercase with spaces", Listener{Env: "  PROD  "}, "prod"},
		{"staging env uppercase", Listener{Env: "STAGING"}, "staging"},
		{"custom env mixed", Listener{Env: "CustomEnv"}, "customenv"},
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
		{"whitespace name defaults to unknown-service", Listener{NameService: "   "}, "unknown-service"},
		{"with name", Listener{NameService: "my-service"}, "my-service"},
		{"with name and whitespace", Listener{NameService: "  my-service  "}, "my-service"},
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
