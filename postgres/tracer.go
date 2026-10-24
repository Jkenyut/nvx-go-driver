package postgres

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type queryCtxKey struct{}

type querySpanMeta struct {
	span      trace.Span
	startTime time.Time
	operation string
}

type privateTracer struct {
	tracer       trace.Tracer
	durationHist metric.Float64Histogram
}

func newPrivateTracer() *privateTracer {
	tracer := otel.Tracer("nvx-go-driver/postgres")
	meter := otel.Meter("nvx-go-driver/postgres")

	durationHist, _ := meter.Float64Histogram(
		"db.client.operation.duration",
		metric.WithDescription("Duration of PostgreSQL client queries"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
	)

	return &privateTracer{
		tracer:       tracer,
		durationHist: durationHist,
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
	if ctx == nil {
		ctx = context.Background()
	}

	op := extractSQLOperation(data.SQL)
	spanName := "DB " + op

	stmt := data.SQL
	if len(stmt) > 256 {
		stmt = stmt[:256] + "... [TRUNCATED]"
	}

	ctx, span := t.tracer.Start(
		ctx,
		spanName,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system", "postgresql"),
			attribute.String("db.operation", op),
			attribute.String("db.statement", stmt),
		),
	)

	return context.WithValue(ctx, queryCtxKey{}, &querySpanMeta{
		span:      span,
		startTime: time.Now(),
		operation: op,
	})
}

func (t *privateTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	meta, ok := ctx.Value(queryCtxKey{}).(*querySpanMeta)
	if !ok || meta == nil || meta.span == nil {
		span := trace.SpanFromContext(ctx)
		if span != nil {
			defer span.End()
			if data.Err != nil && !errors.Is(data.Err, pgx.ErrNoRows) {
				span.RecordError(data.Err)
				span.SetStatus(codes.Error, data.Err.Error())
			}
		}
		return
	}

	elapsed := time.Since(meta.startTime).Seconds()

	attrs := []attribute.KeyValue{
		attribute.String("db.system", "postgresql"),
		attribute.String("db.operation", meta.operation),
	}

	if data.Err != nil && !errors.Is(data.Err, pgx.ErrNoRows) {
		meta.span.RecordError(data.Err)
		meta.span.SetStatus(codes.Error, data.Err.Error())
		attrs = append(attrs, attribute.Bool("error", true))
	} else {
		meta.span.SetStatus(codes.Ok, "")
	}

	if t.durationHist != nil {
		t.durationHist.Record(ctx, elapsed, metric.WithAttributes(attrs...))
	}

	meta.span.End()
}
