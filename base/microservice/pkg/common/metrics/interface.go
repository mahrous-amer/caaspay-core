package metrics

import "time"

// --- Metrics ---
type MetricsInterface interface {
	Increment(metricName string, delta ...int64)
	IncrementTagged(name string, tags ...string)
	RecordLatency(duration time.Duration)
	RecordTiming(operation string, duration time.Duration)
	IncrementError()
	TrackActiveRequests(delta int64)
	ObserveHistogram(name string, value float64, tags ...string)
	Shutdown()
}

// ObservabilityConfig contains observability settings.
type ObservabilityConfig struct {
	TracingEnabled  bool   `mapstructure:"tracing_enabled"` // OpenTelemetry support
	OpentracingHost string `mapstructure:"opentracing_host"`
	OpentracingPort int    `mapstructure:"opentracing_port"`
	MetricsAdapter  string `mapstructure:"metrics_adapter"`
	MetricsHost     string `mapstructure:"metrics_host"`
	MetricsPort     int    `mapstructure:"metrics_port"`
	LogLevelMetrics bool   `mapstructure:"log_level_metrics"` // Toggle log-based metrics
	Env             string `yaml:"env"`
}
