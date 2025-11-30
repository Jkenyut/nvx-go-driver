// logger/logger.go
package logger

import (
	"io"
	"os"

	"github.com/Jkenyut/nvx-go-driver/config"
	"github.com/rs/zerolog"
)

// InitFromConfig — VERSI TERBAIK: langsung dari config listener
func InitFromConfig(cfg config.Listener) {
	env := cfg.Environment() // otomatis ambil dari config, fallback ke "development"

	var writer io.Writer
	if env == "production" || env == "prod" {
		writer = os.Stdout // JSON untuk Loki/ELK
	} else {
		writer = zerolog.ConsoleWriter{
			Out:        os.Stderr,
			TimeFormat: "15:04:05",
			NoColor:    false,
		}
	}

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	// LOG_LEVEL dari env var tetap bisa override
	if level := os.Getenv("LOG_LEVEL"); level != "" {
		if l, err := zerolog.ParseLevel(level); err == nil {
			zerolog.SetGlobalLevel(l)
		}
	} else {
		// Default berdasarkan env
		if env == "production" {
			zerolog.SetGlobalLevel(zerolog.InfoLevel)
		} else {
			zerolog.SetGlobalLevel(zerolog.DebugLevel) // dev/staging = debug
		}
	}

	log := zerolog.New(writer).
		With().
		Timestamp().
		Str("service", cfg.ServiceName()).
		Str("env", env).
		Int("port", cfg.Port).
		Logger()

	zerolog.DefaultContextLogger = &log
}

// L(), Debug(), Info(), dll tetap sama
func L() *zerolog.Logger {
	if zerolog.DefaultContextLogger == nil {
		InitFromConfig(config.Listener{})
	}
	return zerolog.DefaultContextLogger
}

var (
	Debug = func() *zerolog.Event { return L().Debug() }
	Info  = func() *zerolog.Event { return L().Info() }
	Warn  = func() *zerolog.Event { return L().Warn() }
	Error = func() *zerolog.Event { return L().Error() }
	Fatal = func() *zerolog.Event { return L().Fatal() }
	Panic = func() *zerolog.Event { return L().Panic() }
)
