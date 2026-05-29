package config

// Environment returns the environment string.
func (l *Listener) Environment() string {
	if l.Env != "" {
		return l.Env
	}
	return "development"
}

// ServiceName returns the name of the service.
func (l *Listener) ServiceName() string {
	if l.NameService != "" {
		return l.NameService
	}
	return "unknown-service"
}
