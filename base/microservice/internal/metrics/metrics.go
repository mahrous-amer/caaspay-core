package metrics

import (
	"context"
	"fmt"
	"log"
	"time"

	"caaspay-core/api/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
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
	cfg         *config.MetricsConfig
}

// NewMetrics initializes OpenTelemetry with Prometheus and DataDog based on config.
func NewMetrics(cfg *config.MetricsConfig) *Metrics {
	if !cfg.Enabled {
		log.Println("📉 Metrics disabled in configuration")
		return &Metrics{}
	}

	if cfg.Provider == "prometheus" || cfg.Provider == "both" {
		setupPrometheus(cfg)
	}

	if cfg.Provider == "datadog" || cfg.Provider == "both" {
		setupDataDog(cfg)
	}

	// Create OpenTelemetry meter
	meter = otel.Meter(cfg.ServiceName)

	// Register metrics
	requests, _ = meter.Int64Counter("service_requests")
	latency, _ = meter.Float64Histogram("request_latency")
	errorCounter, _ = meter.Int64Counter("request_errors")
	activeReqs, _ = meter.Int64UpDownCounter("active_requests")

	log.Println("✅ Metrics successfully initialized")
	return &Metrics{
		ServiceName: cfg.ServiceName,
		cfg:         cfg,
	}
}

// setupPrometheus configures Prometheus metrics exporter
func setupPrometheus(cfg *config.MetricsConfig) {
	exporter, err := prometheus.New()
	if err != nil {
		log.Fatalf("❌ Failed to initialize Prometheus exporter: %v", err)
	}
	provider := metric.NewMeterProvider(metric.WithReader(exporter))
	otel.SetMeterProvider(provider)

	log.Printf("📡 Prometheus metrics enabled on port %d\n", cfg.PrometheusPort)
	go func() {
		http.Handle("/metrics", exporter)
		if err := http.ListenAndServe(fmt.Sprintf(":%d", cfg.PrometheusPort), nil); err != nil {
			log.Fatalf("❌ Failed to start Prometheus endpoint: %v", err)
		}
	}()
}

// setupDataDog configures DataDog metrics and tracing
func setupDataDog(cfg *config.MetricsConfig) {
	tracer.Start(
		tracer.WithService(cfg.ServiceName),
		tracer.WithEnv(cfg.Env),
	)
	log.Printf("📡 DataDog metrics enabled at %s\n", cfg.DataDogAddr)
}

// Increment increases request count.
func (m *Metrics) Increment(ctx context.Context, metricName string) {
	_, span := tracer.StartSpanFromContext(ctx, metricName, tracer.SpanType(ext.SpanTypeWeb))
	defer span.Finish()

	requests.Add(ctx, 1)
}

// RecordLatency measures request duration.
func (m *Metrics) RecordLatency(ctx context.Context, duration time.Duration) {
	latency.Record(ctx, duration.Seconds())
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
	if m.cfg.Provider == "datadog" || m.cfg.Provider == "both" {
		tracer.Stop()
	}
}
