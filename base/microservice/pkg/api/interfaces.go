package api

import (
	"context"
	"time"

	"github.com/caaspay/caaspay-core/internal/config"
)

// --- Service Lifecycle Interface ---
type ServiceInterface interface {
	Start() error
	Stop() error
	HealthCheck() error
}

// --- Logger ---
type LoggerInterface interface {
	Info(message string, fields map[string]interface{})
	Error(message string, fields map[string]interface{})
	Warn(message string, fields map[string]interface{})
	Debug(message string, fields map[string]interface{})
}

// --- Metrics ---
type MetricsInterface interface {
	Increment(metricName string, delta ...int64)
	RecordLatency(duration time.Duration)
	RecordTiming(operation string, duration time.Duration)
	IncrementError()
	TrackActiveRequests(delta int64)
	Shutdown()
}

// --- Storage abstraction ---
type StorageInterface interface {
	Get(key string) (interface{}, error)
	Set(key string, value interface{}) error
}

// --- Compliance tracking abstraction ---
type ComplianceInterface interface {
	TrackEvent(label string)
}

type ComplianceReporterInterface interface {
	TrackEvent(event string)
}

// --- Validator abstraction ---
type ValidatorInterface interface {
	ValidateStruct(input interface{}) error
}

// SupervisorInterface defines how the framework manages goroutines.
type SupervisorInterface interface {
	// Go launches a managed goroutine that can report failure.
	Go(name string, fn func(ctx context.Context) error)
	GoLoop(name string, interval time.Duration, fn func(ctx context.Context) error)

	// WaitAndShutdown blocks until an error or shutdown occurs, and runs shutdown logic.
	WaitAndShutdown(onShutdown func())

	// Shutdown triggers graceful cancellation of all supervised goroutines.
	Shutdown()

	// Done returns a channel closed once all goroutines have exited.
	Done() <-chan struct{}

	// StopAll is optionally used for forced termination or post-processing logic.
	StopAll()
}

// --- Framework Context (Container) ---
type FrameworkContextInterface interface {
	Logger() LoggerInterface
	Metrics() MetricsInterface
	Transport() TransportInterface
	Storage() StorageInterface
	Compliance() ComplianceInterface
	Config() *config.Config
	Context() context.Context
	ServiceConfig() *config.ServiceConfig
	ServiceName() string
	Validator() ValidatorInterface
	IsHealthy() bool
	Service() ServiceInterface
	SetService(s ServiceInterface)
	Supervisor() SupervisorInterface
	BuildStreamName(kind StreamType, service, method string) string
	BuildRPCStreamName(method string, serviceName ...string) string
	RequestRPC(stream string, input any, output any, timeout time.Duration) error
}

// TransportInterface defines the messaging transport interface for pluggable broker backends.
type TransportInterface interface {
	Publish(stream string, data []byte) error
	Request(stream string, msg *TransportMessage, timeout time.Duration) ([]byte, error)
	Subscribe(consumerGroup string, stream string, handler HandlerFunc) error
	Close() error
	IsHealthy() bool
}
