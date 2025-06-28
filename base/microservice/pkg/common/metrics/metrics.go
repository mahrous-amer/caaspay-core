package metrics

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/caaspay/caaspay-core/pkg/common/logger"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

type Metrics struct {
	ctx         context.Context
	logger      logger.LoggerInterface
	serviceName string
	cfg         *ObservabilityConfig
	meter       metric.Meter
	enabled     bool

	// dynamic registry
	counters      map[string]metric.Int64Counter
	histograms    map[string]metric.Float64Histogram
	activeGauges  map[string]metric.Int64UpDownCounter
	registryMutex sync.Mutex
}

func NewMetrics(ctx context.Context, serviceName string, cfg *ObservabilityConfig, logger logger.LoggerInterface) (*Metrics, error) {
	m := &Metrics{
		ctx:           ctx,
		logger:        logger,
		serviceName:   serviceName,
		cfg:           cfg,
		enabled:       cfg.TracingEnabled,
		counters:      make(map[string]metric.Int64Counter),
		histograms:    make(map[string]metric.Float64Histogram),
		activeGauges:  make(map[string]metric.Int64UpDownCounter),
		registryMutex: sync.Mutex{},
	}

	if !cfg.TracingEnabled {
		logger.Info(ctx, "📉 Metrics disabled in configuration", nil)
		return m, nil
	}

	if cfg.MetricsAdapter == "prometheus" || cfg.MetricsAdapter == "both" {
		if err := setupPrometheus(); err != nil {
			return nil, err
		}
	}

	if cfg.MetricsAdapter == "datadog" || cfg.MetricsAdapter == "both" {
		tracer.Start(
			tracer.WithService(cfg.MetricsHost),
			tracer.WithEnv("development"),
		)
		logger.Info(ctx, "📡 DataDog metrics enabled", map[string]interface{}{
			"adapter": "statsd",
		})
	}

	m.meter = otel.Meter(serviceName)
	logger.Info(ctx, "✅ Metrics successfully initialized", map[string]interface{}{
		"adapter": cfg.MetricsAdapter,
	})

	return m, nil
}

func setupPrometheus() error {
	exporter, err := prometheus.New()
	if err != nil {
		return fmt.Errorf("failed to initialize Prometheus exporter: %w", err)
	}
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	otel.SetMeterProvider(provider)
	return nil
}

// getOrCreateCounter safely registers a counter if not already created
func (m *Metrics) getOrCreateCounter(name string) metric.Int64Counter {
	m.registryMutex.Lock()
	defer m.registryMutex.Unlock()

	if c, ok := m.counters[name]; ok {
		return c
	}
	counter, err := m.meter.Int64Counter(name)
	if err != nil {
		m.logger.Warn(m.ctx, "Failed to register counter", map[string]interface{}{"name": name, "error": err.Error()})
		return nil
	}
	m.counters[name] = counter
	return counter
}

// getOrCreateHistogram safely registers a histogram if not already created
func (m *Metrics) getOrCreateHistogram(name string) metric.Float64Histogram {
	m.registryMutex.Lock()
	defer m.registryMutex.Unlock()

	if h, ok := m.histograms[name]; ok {
		return h
	}
	hist, err := m.meter.Float64Histogram(name)
	if err != nil {
		m.logger.Warn(m.ctx, "Failed to register histogram", map[string]interface{}{"name": name, "error": err.Error()})
		return nil
	}
	m.histograms[name] = hist
	return hist
}

// getOrCreateGauge safely registers a gauge if not already created
func (m *Metrics) getOrCreateGauge(name string) metric.Int64UpDownCounter {
	m.registryMutex.Lock()
	defer m.registryMutex.Unlock()

	if g, ok := m.activeGauges[name]; ok {
		return g
	}
	gauge, err := m.meter.Int64UpDownCounter(name)
	if err != nil {
		m.logger.Warn(m.ctx, "Failed to register gauge", map[string]interface{}{"name": name, "error": err.Error()})
		return nil
	}
	m.activeGauges[name] = gauge
	return gauge
}

// Increment increments a counter metric.
func (m *Metrics) Increment(name string, delta ...int64) {
	if !m.enabled {
		return
	}
	value := int64(1)
	if len(delta) > 0 {
		value = delta[0]
	}
	counter := m.getOrCreateCounter(name)
	if counter != nil {
		counter.Add(m.ctx, value)
	}
}

// IncrementTagged adds a counter value with tags.
func (m *Metrics) IncrementTagged(name string, tags ...string) {
	if !m.enabled {
		return
	}
	counter := m.getOrCreateCounter(name)
	if counter == nil {
		m.logger.Warn(m.ctx, "⚠️ Unknown metric for IncrementTagged", map[string]interface{}{"metric": name})
		return
	}
	attrs := make([]attribute.KeyValue, 0, len(tags)/2)
	for i := 0; i+1 < len(tags); i += 2 {
		attrs = append(attrs, attribute.String(tags[i], tags[i+1]))
	}
	counter.Add(m.ctx, 1, metric.WithAttributes(attrs...))
}

// ObserveHistogram records a float value to a histogram with optional tags.
func (m *Metrics) ObserveHistogram(name string, value float64, tags ...string) {
	if !m.enabled {
		return
	}
	hist := m.getOrCreateHistogram(name)
	if hist == nil {
		m.logger.Warn(m.ctx, "⚠️ Unknown metric for ObserveHistogram", map[string]interface{}{"metric": name})
		return
	}
	attrs := make([]attribute.KeyValue, 0, len(tags)/2)
	for i := 0; i+1 < len(tags); i += 2 {
		attrs = append(attrs, attribute.String(tags[i], tags[i+1]))
	}
	hist.Record(m.ctx, value, metric.WithAttributes(attrs...))
}

func (m *Metrics) RecordLatency(duration time.Duration) {
	m.ObserveHistogram("request_latency", duration.Seconds())
}

func (m *Metrics) RecordTiming(operation string, duration time.Duration) {
	m.ObserveHistogram("operation_duration", duration.Seconds(), "operation", operation)
}

func (m *Metrics) IncrementError() {
	m.Increment("request_errors")
}

func (m *Metrics) TrackActiveRequests(delta int64) {
	gauge := m.getOrCreateGauge("active_requests")
	if gauge != nil {
		gauge.Add(m.ctx, delta)
	}
}

func (m *Metrics) Shutdown() {
	if m.cfg.MetricsAdapter == "datadog" || m.cfg.MetricsAdapter == "both" {
		tracer.Stop()
	}
}
