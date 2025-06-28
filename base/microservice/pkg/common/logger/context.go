package logger

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/caaspay/caaspay-core/pkg/common/fwctx"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type contextAwareHandler struct {
	next        slog.Handler
	serviceName string
}

func (h *contextAwareHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *contextAwareHandler) Handle(ctx context.Context, r slog.Record) error {
	r.AddAttrs(
		slog.String("service", h.serviceName),
		slog.String("timestamp", time.Now().Format(time.RFC3339)),
	)

	for _, key := range []fwctx.ContextKey{
		fwctx.CtxKeyMessageID,
		fwctx.CtxKeyStream,
		fwctx.CtxKeyRStream,
		fwctx.CtxKeyTraceID,
		fwctx.CtxKeySpanID,
		fwctx.CtxKeyUserID,
		fwctx.CtxKeyIP,
		fwctx.CtxKeyLocale,
		fwctx.CtxKeySource,
	} {
		if val := ctx.Value(key); val != nil {
			r.AddAttrs(slog.String(string(key), fmt.Sprintf("%v", val)))
		}
	}

	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		var attrs []attribute.KeyValue
		r.Attrs(func(attr slog.Attr) bool {
			if attr.Value.Kind() == slog.KindString {
				attrs = append(attrs, attribute.String(attr.Key, attr.Value.String()))
			} else {
				attrs = append(attrs, attribute.String(attr.Key, fmt.Sprintf("%v", attr.Value.Any())))
			}
			return true
		})
		span.SetAttributes(attrs...)
	}

	return h.next.Handle(ctx, r)
}

func (h *contextAwareHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &contextAwareHandler{
		next:        h.next.WithAttrs(attrs),
		serviceName: h.serviceName,
	}
}

func (h *contextAwareHandler) WithGroup(name string) slog.Handler {
	return &contextAwareHandler{
		next:        h.next.WithGroup(name),
		serviceName: h.serviceName,
	}
}
