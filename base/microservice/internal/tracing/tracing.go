package tracing

import (
	"context"
	"fmt"
	"log"
	"time"

	"caaspay-api-go/api/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/exporters/trace/ddtrace"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/semconv/v1.4.0"
)

// Tracer global variable
var Tracer trace.Tracer

// InitTracing initializes tracing using provided observability config.
func InitTracing(serviceName string, cfg config.ObservabilityConfig) func() {
	if !cfg.TracingEnabled {
		log.Println("⚠️ Tracing is disabled in configuration.")
		return func() {} // No-op shutdown
	}

	var tp *trace.TracerProvider
	var err error

	switch cfg.MetricsAdapter {
	case "jaeger":
		tp, err = setupJaeger(serviceName, cfg)
	case "datadog":
		tp, err = setupDatadog(serviceName, cfg)
	default:
		log.Println("⚠️ No valid tracing adapter provided. Tracing disabled.")
		return func() {} // No-op shutdown
	}

	if err != nil {
		log.Printf("❌ Failed to initialize tracing: %v", err)
		return func() {} // No-op shutdown
	}

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	Tracer = tp.Tracer(serviceName)

	return func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := tp.Shutdown(shutdownCtx); err != nil {
			log.Printf("⚠️ Failed to shutdown tracer provider: %v", err)
		}
	}
}

// setupJaeger initializes a Jaeger exporter based on config values.
func setupJaeger(serviceName string, cfg config.ObservabilityConfig) (*trace.TracerProvider, error) {
	jaegerEndpoint := fmt.Sprintf("http://%s:%d/api/traces", cfg.OpentracingHost, cfg.OpentracingPort)

	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(jaegerEndpoint)))
	if err != nil {
		return nil, err
	}

	tp := trace.NewTracerProvider(
		trace.WithBatcher(exp),
		trace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(serviceName),
		)),
	)

	log.Println("✅ Jaeger tracing initialized")
	return tp, nil
}

// setupDatadog initializes a Datadog exporter using observability config.
func setupDatadog(serviceName string, cfg config.ObservabilityConfig) (*trace.TracerProvider, error) {
	datadogAddr := fmt.Sprintf("http://%s:%d/v0.4/traces", cfg.MetricsHost, cfg.MetricsPort)

	exp, err := ddtrace.NewExporter(ddtrace.WithAgentAddr(datadogAddr))
	if err != nil {
		return nil, err
	}

	tp := trace.NewTracerProvider(
		trace.WithBatcher(exp),
		trace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(serviceName),
		)),
	)

	log.Println("✅ Datadog tracing initialized")
	return tp, nil
}

// StartSpan starts a new tracing span.
func StartSpan(ctx context.Context, name string) (context.Context, trace.Span) {
	return Tracer.Start(ctx, name)
}
