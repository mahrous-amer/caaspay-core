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
	serviceName string
	cfg         *config.ObservabilityConfig
	ctx         context.Context
}

// NewMetrics initializes OpenTelemetry with Prometheus and DataDog based on config.
func NewMetrics(ctx context.Context, serviceName string, cfg *config.ObservabilityConfig) (*Metrics, error) {
	if !cfg.TracingEnabled {
		log.Println("📉 Metrics disabled in configuration")
		return &Metrics{ctx: ctx}, nil
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
		serviceName: serviceName,
		cfg:         cfg,
		ctx:         ctx,
	}, nil
}

// setupPrometheus configures Prometheus metrics exporter.
func setupPrometheus(cfg *config.ObservabilityConfig) error {
	exporter, err := prometheus.New()
	if err != nil {
		return fmt.Errorf("failed to initialize Prometheus exporter: %v", err)
	}

	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	otel.SetMeterProvider(provider)
	return nil
}

// setupDataDog configures DataDog metrics and tracing.
func setupDataDog(cfg *config.ObservabilityConfig) {
	tracer.Start(
		tracer.WithService(cfg.MetricsHost),
		tracer.WithEnv("development"),
	)
	log.Printf("📡 DataDog metrics enabled at %s\n", cfg.MetricsHost)
}

// Increment increases request count (default 1 unless delta provided).
func (m *Metrics) Increment(metricName string, delta ...int64) {
	d := int64(1)
	if len(delta) > 0 {
		d = delta[0]
	}
	span, _ := tracer.StartSpanFromContext(m.ctx, metricName)
	defer span.Finish()

	requests.Add(m.ctx, d)
}

// RecordLatency measures request duration.
func (m *Metrics) RecordLatency(duration time.Duration) {
	latency.Record(m.ctx, duration.Seconds())
}

// RecordTiming records the duration of an operation with tagging.
func (m *Metrics) RecordTiming(operation string, duration time.Duration) {
	latency.Record(m.ctx, duration.Seconds(), metric.WithAttributes(
		attribute.String("operation", operation),
	))
}

// IncrementError tracks failed requests.
func (m *Metrics) IncrementError() {
	errorCounter.Add(m.ctx, 1)
}

// TrackActiveRequests adjusts the active request count.
func (m *Metrics) TrackActiveRequests(delta int64) {
	activeReqs.Add(m.ctx, delta)
}

// Shutdown cleans up DataDog tracing.
func (m *Metrics) Shutdown() {
	if m.cfg.MetricsAdapter == "datadog" || m.cfg.MetricsAdapter == "both" {
		tracer.Stop()
	}
}
