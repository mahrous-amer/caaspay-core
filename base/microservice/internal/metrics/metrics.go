package metrics

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/caaspay/caaspay-core/internal/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

var (
	meter        metric.Meter
	requests     metric.Int64Counter
	latency      metric.Float64Histogram
	errorCounter metric.Int64Counter
	activeReqs   metric.Int64UpDownCounter
)

// Metrics manages monitoring for services.
type Metrics struct {
	ServiceName string
	cfg         *config.ObservabilityConfig
}

// NewMetrics initializes OpenTelemetry with Prometheus and DataDog based on config.
func NewMetrics(serviceName string, cfg *config.ObservabilityConfig) (*Metrics, error) {
	if !cfg.TracingEnabled {
		log.Println("📉 Metrics disabled in configuration")
		return &Metrics{}, nil
	}

	if cfg.MetricsAdapter == "prometheus" || cfg.MetricsAdapter == "both" {
		if err := setupPrometheus(cfg); err != nil {
			return nil, err
		}
	}

	if cfg.MetricsAdapter == "datadog" || cfg.MetricsAdapter == "both" {
		setupDataDog(cfg)
	}

	// Create OpenTelemetry meter
	meter = otel.Meter(serviceName)

	// Register metrics
	requests, _ = meter.Int64Counter("service_requests")
	latency, _ = meter.Float64Histogram("request_latency")
	errorCounter, _ = meter.Int64Counter("request_errors")
	activeReqs, _ = meter.Int64UpDownCounter("active_requests")

	log.Println("✅ Metrics successfully initialized")
	return &Metrics{
		ServiceName: serviceName,
		cfg:         cfg,
	}, nil
}

// setupPrometheus configures Prometheus metrics exporter
func setupPrometheus(cfg *config.ObservabilityConfig) error {
	exporter, err := prometheus.New()
	if err != nil {
		return fmt.Errorf("failed to initialize Prometheus exporter: %v", err)
	}

	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	otel.SetMeterProvider(provider)

	// Prometheus server is handled in service/health_server.go if needed.

	return nil
}

// setupDataDog configures DataDog metrics and tracing
func setupDataDog(cfg *config.ObservabilityConfig) {
	tracer.Start(
		tracer.WithService(cfg.MetricsHost),
		tracer.WithEnv("development"),
	)
	log.Printf("📡 DataDog metrics enabled at %s\n", cfg.MetricsHost)
}

// Increment increases request count.
func (m *Metrics) Increment(ctx context.Context, metricName string) {
	span, _ := tracer.StartSpanFromContext(ctx, metricName)
	defer span.Finish()

	requests.Add(ctx, 1)
}

// RecordLatency measures request duration.
func (m *Metrics) RecordLatency(ctx context.Context, duration time.Duration) {
	latency.Record(ctx, duration.Seconds())
}

// RecordTiming records the duration of an operation.
func (m *Metrics) RecordTiming(ctx context.Context, operation string, duration time.Duration) {
	latency.Record(ctx, duration.Seconds(), metric.WithAttributes(
		attribute.String("operation", operation),
	))
}

// IncrementError tracks failed requests.
func (m *Metrics) IncrementError(ctx context.Context) {
	errorCounter.Add(ctx, 1)
}

// TrackActiveRequests maintains active request count.
func (m *Metrics) TrackActiveRequests(ctx context.Context, delta int64) {
	activeReqs.Add(ctx, delta)
}

// Shutdown cleans up DataDog tracing.
func (m *Metrics) Shutdown() {
	if m.cfg.MetricsAdapter == "datadog" || m.cfg.MetricsAdapter == "both" {
		tracer.Stop()
	}
}
