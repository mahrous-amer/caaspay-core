package tracing

import (
	"context"
	"fmt"
	"time"

	"github.com/caaspay/caaspay-core/internal/config"
	"github.com/caaspay/caaspay-core/internal/logging"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"

	ddtracer "gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

// TracerManager holds tracer dependencies and context.
type TracerManager struct {
	ctx      context.Context
	tracer   trace.Tracer
	shutdown func()
	logger   *logging.Logger
}

// NewTracerManager initializes OpenTelemetry tracing with proper logging and shutdown.
func NewTracerManager(ctx context.Context, serviceName string, cfg *config.ObservabilityConfig, logger *logging.Logger) (*TracerManager, error) {
	if !cfg.TracingEnabled {
		logger.Warn("⚠️ Tracing is disabled in configuration", nil)
		return &TracerManager{
			ctx:      ctx,
			tracer:   trace.NewNoopTracerProvider().Tracer(serviceName),
			shutdown: func() {},
			logger:   logger,
		}, nil
	}

	var (
		tp  *sdktrace.TracerProvider
		err error
	)

	switch cfg.MetricsAdapter {
	case "jaeger":
		tp, err = setupJaeger(serviceName, cfg, logger)
	case "datadog":
		tp, err = setupDatadog(serviceName, cfg, logger)
	default:
		logger.Warn("⚠️ No valid tracing adapter provided. Tracing disabled", nil)
		return &TracerManager{
			ctx:      ctx,
			tracer:   trace.NewNoopTracerProvider().Tracer(serviceName),
			shutdown: func() {},
			logger:   logger,
		}, nil
	}

	if err != nil {
		logger.Error("❌ Failed to initialize tracing", map[string]interface{}{"error": err.Error()})
		return &TracerManager{
			ctx:      ctx,
			tracer:   trace.NewNoopTracerProvider().Tracer(serviceName),
			shutdown: func() {},
			logger:   logger,
		}, nil
	}

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	logger.Info("✅ Tracing initialized successfully", map[string]interface{}{
		"adapter": cfg.MetricsAdapter,
	})

	return &TracerManager{
		ctx:    ctx,
		tracer: tp.Tracer(serviceName),
		logger: logger,
		shutdown: func() {
			shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			if err := tp.Shutdown(shutdownCtx); err != nil {
				logger.Error("⚠️ Failed to shutdown tracer provider", map[string]interface{}{
					"error": err.Error(),
				})
			}
		},
	}, nil
}

// StartSpan creates a new span using the internal tracer and context.
func (tm *TracerManager) StartSpan(name string) (context.Context, trace.Span) {
	return tm.tracer.Start(tm.ctx, name)
}

// Shutdown shuts down the tracer provider gracefully.
func (tm *TracerManager) Shutdown() {
	tm.shutdown()
}

// setupJaeger configures Jaeger exporter and tracer provider.
func setupJaeger(serviceName string, cfg *config.ObservabilityConfig, logger *logging.Logger) (*sdktrace.TracerProvider, error) {
	endpoint := fmt.Sprintf("http://%s:%d/api/traces", cfg.OpentracingHost, cfg.OpentracingPort)

	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(endpoint)))
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

	logger.Info("✅ Jaeger tracing initialized", map[string]interface{}{
		"endpoint": endpoint,
	})

	return tp, nil
}

// setupDatadog configures Datadog tracer and returns a tracer provider.
func setupDatadog(serviceName string, cfg *config.ObservabilityConfig, logger *logging.Logger) (*sdktrace.TracerProvider, error) {
	ddtracer.Start(
		ddtracer.WithService(serviceName),
		ddtracer.WithEnv(cfg.Env),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(serviceName),
		)),
	)

	logger.Info("✅ Datadog tracing initialized", map[string]interface{}{
		"env": cfg.Env,
	})

	return tp, nil
}
