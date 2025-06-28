package api

import (
	"context"
	"github.com/caaspay/caaspay-core/pkg/common/logger"
	"github.com/caaspay/caaspay-core/pkg/common/metrics"
	"github.com/caaspay/caaspay-core/pkg/common/storage"
	"github.com/caaspay/caaspay-core/pkg/common/supervisor"
	"github.com/caaspay/caaspay-core/pkg/common/transport"
	"github.com/caaspay/caaspay-core/pkg/common/validation"
	"net/http"
	"time"
)

// --- Service Lifecycle Interface ---
type ServiceInterface interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	HealthCheck(ctx context.Context) error
}

// --- Compliance tracking abstraction ---
type ComplianceInterface interface {
	TrackEvent(label string)
}

type ComplianceReporterInterface interface {
	TrackEvent(event string)
}

// --- Framework Context (Container) ---
type FrameworkContextInterface interface {
	Logger() logger.LoggerInterface
	Metrics() metrics.MetricsInterface
	Transport() transport.TransportInterface
	Supervisor() supervisor.SupervisorInterface
	Validator() validation.ValidatorInterface
	Storage() storage.StorageInterface
	Compliance() ComplianceInterface
	Config() *Config
	Context() context.Context
	ServiceConfig() *ServiceConfig
	ServiceName() string
	IsHealthy() bool
	Service() ServiceInterface
	SetService(s ServiceInterface)
	BuildStreamName(kind transport.StreamType, service, method string) string
	BuildRPCStreamName(method string, serviceName ...string) string
	RequestRPC(stream string, input any, output any, timeout time.Duration) error
	//RequestHTTP(ctx context.Context, method, url string, body []byte, headers map[string]string) (*http.Response, error)
	RequestHTTP(
		ctx context.Context,
		method string,
		url string,
		input any,
		headers map[string]string,
		output any,
	) error
	RequestHTTPRaw(
		ctx context.Context,
		method string,
		url string,
		body []byte,
		headers map[string]string,
	) (*http.Response, []byte, error)
}

type HTTPClientInterface interface {
	//Request(ctx context.Context, method, url string, body []byte, headers map[string]string) (*http.Response, error)
	RequestHTTP(
		ctx context.Context,
		method string,
		url string,
		input any,
		headers map[string]string,
		output any,
	) error
	RequestHTTPRaw(
		ctx context.Context,
		method string,
		url string,
		body []byte,
		headers map[string]string,
	) (*http.Response, []byte, error)
}
