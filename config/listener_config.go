package config

import "strings"

// Environment returns the normalized lowercase environment string.
// Defaults to "development" if empty or whitespace-only.
func (l *Listener) Environment() string {
	if l != nil {
		if env := strings.ToLower(strings.TrimSpace(l.Env)); env != "" {
			return env
		}
	}
	return "development"
}

// ServiceName returns the trimmed name of the service.
// Defaults to "unknown-service" if empty or whitespace-only.
func (l *Listener) ServiceName() string {
	if l != nil {
		if name := strings.TrimSpace(l.NameService); name != "" {
			return name
		}
	}
	return "unknown-service"
}
