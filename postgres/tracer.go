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

func (t *privateTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	// Extract basic operation name to prevent full SQL PII leak
	op := "QUERY"
	if fields := strings.Fields(data.SQL); len(fields) > 0 {
		op = strings.ToUpper(fields[0])
	}

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
