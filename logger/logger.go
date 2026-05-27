// Package logger provides a centralized, configuration-driven structured logging
// setup using zerolog. It automatically adapts to the runtime environment:
//
//   - production/prod  → JSON output to stdout (ideal for Loki, ELK, CloudWatch)
//   - development/staging/other → Pretty colored console output to stderr
//
// The logger is initialized once at application startup via InitFromConfig().
// All subsequent log calls use the global logger through convenience functions.
//
// Features:
//   - Environment-aware output format
//   - Automatic service name, environment, and port inclusion
//   - LOG_LEVEL environment variable override
//   - Default level: Debug in non-production, Info in production
//   - Human-readable timestamp format: "2006-01-02 15:04:05"
//
// Usage:
//
//	import "your-project/logger"
//	import "your-project/config"
//
//	func main() {
//	    logger.InitFromConfig(appConfig.Listener)
//	    logger.Info().Msg("Application started")
//	}
package logger

import (
	"io"
	"os"
	"time"

	"github.com/Jkenyut/nvx-go-driver/config"
	"github.com/rs/zerolog"
)

// InitFromConfig initializes the global zerolog logger using values from the
// provided Listener configuration.
//
// The function configures:
//   - Output format (JSON vs pretty console) based on cfg.Environment()
//   - Log level (default: Info in production, Debug elsewhere; overridable via LOG_LEVEL env var)
//   - Common fields: timestamp, service name, environment, and listening port
//   - Timestamp format: "2006-01-02 15:04:05" (human-readable)
//
// This function should be called once at application startup before any logging occurs.
func InitFromConfig(cfg config.Listener) {
	env := cfg.Environment() // defaults to "development" if empty

	var writer io.Writer
	if env == "production" || env == "prod" {
		// JSON output for log aggregation systems
		writer = os.Stdout
	} else {
		// Pretty, colored output for local development and staging
		writer = zerolog.ConsoleWriter{
			Out:        os.Stderr,
			TimeFormat: "2006-01-02 15:04:05", // requested human-readable format
			NoColor:    false,
		}
	}

	// Use Unix seconds internally, but display in human format via ConsoleWriter
	zerolog.TimeFieldFormat = time.RFC3339

	// Respect LOG_LEVEL environment variable if set
	if levelStr := os.Getenv("LOG_LEVEL"); levelStr != "" {
		if level, err := zerolog.ParseLevel(levelStr); err == nil {
			zerolog.SetGlobalLevel(level)
		}
	} else {
		// Default level based on environment
		if env == "production" || env == "prod" {
			zerolog.SetGlobalLevel(zerolog.InfoLevel)
		} else {
			zerolog.SetGlobalLevel(zerolog.DebugLevel)
		}
	}

	log := zerolog.New(writer).
		With().
		Timestamp().
		Caller().
		Str("service", cfg.ServiceName()).
		Str("env", env).
		Int("port", cfg.Port).
		Logger()

	zerolog.DefaultContextLogger = &log
}

// L returns the global logger instance.
//
// If the logger has not been initialized (e.g., InitFromConfig was not called),
// it performs a fallback initialization with empty/default values.
func L() *zerolog.Logger {
	if zerolog.DefaultContextLogger == nil {
		InitFromConfig(config.Listener{})
	}
	return zerolog.DefaultContextLogger
}

// Convenience global functions for common log levels.
// These use the global logger and are safe to call from any package.

// Debug creates a debug-level log event.
func Debug() *zerolog.Event { return L().Debug() }

// Info creates an info-level log event.
func Info() *zerolog.Event { return L().Info() }

// Warn creates a warning-level log event.
func Warn() *zerolog.Event { return L().Warn() }

// Error creates an error-level log event.
func Error() *zerolog.Event { return L().Error() }

// Fatal creates a fatal-level log event (logs and calls os.Exit(1)).
func Fatal() *zerolog.Event { return L().Fatal() }

// Panic creates a panic-level log event (logs and panics).
func Panic() *zerolog.Event { return L().Panic() }
