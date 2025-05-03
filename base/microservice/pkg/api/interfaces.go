package api

import (
	"context"
	"time"

	"github.com/caaspay/caaspay-core/internal/config"
	"github.com/caaspay/caaspay-core/internal/transport"
)

// --- Service Lifecycle Interface ---
type ServiceInterface interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	HealthCheck(ctx context.Context) error
}

// --- Logger ---
type LoggerInterface interface {
	Info(ctx context.Context, message string, fields map[string]interface{})
	Error(ctx context.Context, message string, fields map[string]interface{})
	Debug(ctx context.Context, message string, fields map[string]interface{})
}

// --- Metrics ---
type MetricsInterface interface {
	Increment(ctx context.Context, metricName string)
	RecordLatency(ctx context.Context, duration time.Duration)
	RecordTiming(ctx context.Context, operation string, duration time.Duration)
	IncrementError(ctx context.Context)
	TrackActiveRequests(ctx context.Context, delta int64)
	Shutdown()
}

// --- Transport (Publisher/Subscriber abstraction) ---
type TransportInterface interface {
	Publish(ctx context.Context, stream string, payload []byte) error
	Subscribe(stream string, handler transport.HandlerFunc) error
	IsHealthy() bool
}

// --- Storage abstraction ---
type StorageInterface interface {
	Get(ctx context.Context, key string) (interface{}, error)
	Set(ctx context.Context, key string, value interface{}) error
}

// --- Compliance tracking abstraction ---
type ComplianceInterface interface {
	TrackEvent(ctx context.Context, label string)
}

type ComplianceReporterInterface interface {
	TrackEvent(ctx context.Context, event string)
}

// --- Validator abstraction ---
type ValidatorInterface interface {
	ValidateStruct(input interface{}) error
}

// --- Framework Context (Container) ---
type FrameworkContextInterface interface {
	Logger() LoggerInterface
	Metrics() MetricsInterface
	Transport() TransportInterface
	Storage() StorageInterface
	Compliance() ComplianceInterface
	Config() *config.Config
	ServiceConfig() *config.ServiceConfig
	ServiceName() string
	Validator() ValidatorInterface
	IsHealthy() bool
}
