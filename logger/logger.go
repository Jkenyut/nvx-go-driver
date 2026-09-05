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
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/Jkenyut/nvx-go-driver/config"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/diode"
	"go.opentelemetry.io/otel/trace"
)

var (
	closerMu          sync.Mutex
	globalDiodeCloser io.Closer
	initOnce          sync.Once
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
	isProd := env == "production" || env == "prod"

	if isProd {
		// JSON output for log aggregation systems (wrapped to prevent closing os.Stdout)
		writer = io.MultiWriter(os.Stdout)
	} else {
		// Pretty, colored output for local development and staging
		writer = zerolog.ConsoleWriter{
			Out:     os.Stderr,
			NoColor: false,
		}
	}

	// Use RFC3339 seconds internally, but display in human format via ConsoleWriter
	zerolog.TimeFieldFormat = time.RFC3339

	// Respect LOG_LEVEL environment variable if set
	if levelStr := os.Getenv("LOG_LEVEL"); levelStr != "" {
		if level, err := zerolog.ParseLevel(levelStr); err == nil {
			zerolog.SetGlobalLevel(level)
		}
	} else {
		// Default level based on environment
		if isProd {
			zerolog.SetGlobalLevel(zerolog.InfoLevel)
		} else {
			zerolog.SetGlobalLevel(zerolog.DebugLevel)
		}
	}

	diodeSize := 1000
	if isProd {
		diodeSize = 10000
	}
	if sizeStr := os.Getenv("LOG_BUFFER_SIZE"); sizeStr != "" {
		if s, err := strconv.Atoi(sizeStr); err == nil && s > 0 {
			diodeSize = s
		}
	}

	wr := diode.NewWriter(writer, diodeSize, 10*time.Millisecond, func(missed int) {
		_, _ = fmt.Fprintf(os.Stderr, "[logger] warning: diode dropped %d log messages\n", missed)
	})

	closerMu.Lock()
	if globalDiodeCloser != nil {
		_ = globalDiodeCloser.Close()
	}
	globalDiodeCloser = wr
	closerMu.Unlock()

	logContext := zerolog.New(wr).
		With().
		Timestamp().
		Str("service", cfg.ServiceName()).
		Str("env", env).
		Int("port", cfg.Port)

	// Caller is expensive. Only enable it in non-production environments.
	if !isProd {
		logContext = logContext.Caller()
	}

	log := logContext.Logger()
	zerolog.DefaultContextLogger = &log
}

// Close flushes all remaining logs in the buffer and stops the background
// logging goroutine. This should be called during graceful shutdown.
func Close() error {
	closerMu.Lock()
	defer closerMu.Unlock()
	if globalDiodeCloser != nil {
		err := globalDiodeCloser.Close()
		globalDiodeCloser = nil
		return err
	}
	return nil
}

// L returns the global logger instance.
//
// If the logger has not been initialized (e.g., InitFromConfig was not called),
// it performs a fallback initialization with empty/default values.
func L() *zerolog.Logger {
	initOnce.Do(func() {
		if zerolog.DefaultContextLogger == nil {
			InitFromConfig(config.Listener{})
		}
	})
	return zerolog.DefaultContextLogger
}

// Ctx returns a logger instance with OpenTelemetry trace_id and span_id from context.
func Ctx(ctx context.Context) *zerolog.Logger {
	l := L().With().Logger()
	if ctx != nil {
		spanCtx := trace.SpanContextFromContext(ctx)
		if spanCtx.HasTraceID() {
			l = l.With().Str("trace_id", spanCtx.TraceID().String()).Logger()
		}
		if spanCtx.HasSpanID() {
			l = l.With().Str("span_id", spanCtx.SpanID().String()).Logger()
		}
	}
	return &l
}

// Convenience global functions for common log levels.
// These use the global logger and enforce passing context for trace propagation.

// Debug creates a debug-level log event with OpenTelemetry trace_id and span_id.
func Debug(ctx context.Context) *zerolog.Event { return Ctx(ctx).Debug() }

// Info creates an info-level log event with OpenTelemetry trace_id and span_id.
func Info(ctx context.Context) *zerolog.Event { return Ctx(ctx).Info() }

// Warn creates a warning-level log event with OpenTelemetry trace_id and span_id.
func Warn(ctx context.Context) *zerolog.Event { return Ctx(ctx).Warn() }

// Error creates an error-level log event with OpenTelemetry trace_id and span_id.
func Error(ctx context.Context) *zerolog.Event { return Ctx(ctx).Error() }

// Fatal creates a fatal-level log event with OpenTelemetry trace_id and span_id.
func Fatal(ctx context.Context) *zerolog.Event { return Ctx(ctx).Fatal() }

// Panic creates a panic-level log event with OpenTelemetry trace_id and span_id.
func Panic(ctx context.Context) *zerolog.Event { return Ctx(ctx).Panic() }
