package postgres

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type privateTracer struct {
	tracer trace.Tracer
}

func newPrivateTracer() *privateTracer {
	return &privateTracer{
		tracer: otel.Tracer("nvx-go-driver/postgres"),
	}
}

// extractSQLOperation extracts the first SQL keyword without allocating slices.
// It trims whitespace and skips leading block (/* ... */) or line (-- ...) comments.
func extractSQLOperation(sql string) string {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return "QUERY"
	}

	for {
		if strings.HasPrefix(sql, "/*") {
			end := strings.Index(sql, "*/")
			if end == -1 {
				return "QUERY"
			}
			sql = strings.TrimSpace(sql[end+2:])
			continue
		}
		if strings.HasPrefix(sql, "--") {
			end := strings.IndexByte(sql, '\n')
			if end == -1 {
				return "QUERY"
			}
			sql = strings.TrimSpace(sql[end+1:])
			continue
		}
		break
	}

	if sql == "" {
		return "QUERY"
	}

	end := strings.IndexAny(sql, " \t\r\n(;")
	if end == -1 {
		end = len(sql)
	}
	if end > 16 {
		end = 16
	}
	return strings.ToUpper(sql[:end])
}

func (t *privateTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	op := extractSQLOperation(data.SQL)
	ctx, _ = t.tracer.Start(ctx, op, trace.WithSpanKind(trace.SpanKindClient))
	return ctx
}

func (t *privateTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	span := trace.SpanFromContext(ctx)
	defer span.End()

	if data.Err != nil {
		span.RecordError(data.Err)
		span.SetAttributes(attribute.String("db.error", data.Err.Error()))
	}
}
