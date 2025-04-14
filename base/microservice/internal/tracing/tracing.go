package tracing

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/caaspay/caaspay-core/internal/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

// Tracer global variable
var Tracer trace.Tracer

// InitTracing initializes tracing using provided observability config.
func InitTracing(serviceName string, cfg *config.ObservabilityConfig) func() {
	if !cfg.TracingEnabled {
		log.Println("⚠️ Tracing is disabled in configuration.")
		return func() {} // No-op shutdown
	}

	var tp *sdktrace.TracerProvider
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
func setupJaeger(serviceName string, cfg *config.ObservabilityConfig) (*sdktrace.TracerProvider, error) {
	jaegerEndpoint := fmt.Sprintf("http://%s:%d/api/traces", cfg.OpentracingHost, cfg.OpentracingPort)

	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(jaegerEndpoint)))
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(serviceName),
		)),
	)

	log.Println("✅ Jaeger tracing initialized")
	return tp, nil
}

// setupDatadog initializes a Datadog exporter using observability config.
func setupDatadog(serviceName string, cfg *config.ObservabilityConfig) (*sdktrace.TracerProvider, error) {
	// Initialize DataDog tracer
	tracer.Start(
		tracer.WithService(serviceName),
		tracer.WithEnv("production"),
	)
	
	// Create a no-op tracer provider for DataDog since it uses its own tracer
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(serviceName),
		)),
	)

	log.Println("✅ Datadog tracing initialized")
	return tp, nil
}

// StartSpan creates a new span from the context.
func StartSpan(ctx context.Context, name string) (context.Context, trace.Span) {
	return Tracer.Start(ctx, name)
}
