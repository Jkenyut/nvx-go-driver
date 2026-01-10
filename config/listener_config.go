package config

// Tambahkan method biar gampang
func (l Listener) Environment() string {
	if l.Env != "" {
		return l.Env
	}
	return "development"
}

func (l Listener) ServiceName() string {
	if l.NameService != "" {
		return l.NameService
	}
	return "unknown-service"
}
