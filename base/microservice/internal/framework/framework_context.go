package framework

import (
	"context"
	"fmt"
	"time"

	"github.com/caaspay/caaspay-core/internal/compliance"
	"github.com/caaspay/caaspay-core/internal/config"
	"github.com/caaspay/caaspay-core/internal/logging"
	"github.com/caaspay/caaspay-core/internal/metrics"
	"github.com/caaspay/caaspay-core/internal/storage"
	"github.com/caaspay/caaspay-core/internal/transport"
	"github.com/caaspay/caaspay-core/pkg/api"
)

// FrameworkContext encapsulates all framework components.
type FrameworkContext struct {
	config        *config.Config
	logger        api.LoggerInterface
	metrics       api.MetricsInterface
	transport     transport.Transport
	storage       storage.Store
	compliance    api.ComplianceReporterInterface
	serviceName   string
	serviceConfig *config.ServiceConfig
	service       api.ServiceInterface
}

// NewFrameworkContext initializes all framework components and returns a unified context.
func NewFrameworkContext() (*FrameworkContext, error) {
	cfg, err := config.LoadConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	logger := logging.NewLogger(cfg.Framework.ServiceName, cfg.Framework.Logging.Level, cfg.Framework.Logging.RedactSensitive)

	metricsInstance, err := metrics.NewMetrics(cfg.Framework.ServiceName, &cfg.Framework.Observability)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize metrics: %w", err)
	}

	redisCfg := transport.RedisTransportConfig{
		RedisAddr:          cfg.Framework.Transport.RedisAddr,
		UseCompression:     cfg.Framework.Transport.UseCompression,
		UseEncryption:      cfg.Framework.Transport.UseEncryption,
		ServiceReplyStream: fmt.Sprintf("%s_reply", cfg.Framework.ServiceName),
		MaxRetries:         cfg.Framework.Transport.MaxRetries,
		RetryDelay:         cfg.Framework.Transport.RetryDelay,
	}
	redisTransport, err := transport.NewRedisTransport(redisCfg, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize transport: %w", err)
	}

	store, err := storage.NewStore(cfg.Framework.Storage)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize storage: %w", err)
	}

	complianceReporter := compliance.NewComplianceReporter(cfg, metricsInstance)

	serviceConfig := &config.ServiceConfig{}
	svcName := cfg.Framework.ServiceName
	if serviceData, ok := cfg.Service[svcName]; ok {
		if err := config.MapToStruct(serviceData, serviceConfig); err != nil {
			return nil, fmt.Errorf("failed to map config for service %s: %w", svcName, err)
		}
	} else {
		serviceConfig = config.DefaultServiceConfig()
	}

	if cfg.Framework.HealthCheck.HeartbeatEnabled {
		go startHeartbeat(logger, redisTransport, cfg.Framework.HealthCheck.HeartbeatInterval)
	}

	return &FrameworkContext{
		config:        cfg,
		logger:        logger,
		metrics:       metricsInstance,
		transport:     redisTransport,
		storage:       store,
		compliance:    complianceReporter,
		serviceName:   cfg.Framework.ServiceName,
		serviceConfig: serviceConfig,
	}, nil
}

// IsHealthy checks if the framework and its key components are healthy.
func (f *FrameworkContext) IsHealthy() bool {
	if r, ok := f.transport.(interface{ IsHealthy() bool }); ok {
		if !r.IsHealthy() {
			f.logger.Error(context.Background(), "🚨 Transport not healthy", nil)
			return false
		}
	}

	if f.service != nil {
		if svc, ok := f.service.(interface {
			HealthCheck(ctx context.Context) error
		}); ok {
			if err := svc.HealthCheck(context.Background()); err != nil {
				f.logger.Error(context.Background(), "🚨 Service HealthCheck failed", map[string]interface{}{
					"error": err.Error(),
				})
				return false
			}
		}
	}
	return true
}

func startHeartbeat(logger api.LoggerInterface, transport transport.Transport, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ctx := context.Background()
			if transport.IsHealthy() {
				logger.Info(ctx, "💓 Framework heartbeat... Redis is healthy", nil)
			} else {
				logger.Error(ctx, "💔 Framework heartbeat... Redis is NOT healthy", nil)
			}
		}
	}
}

// Public accessors
func (f *FrameworkContext) Config() *config.Config {
	return f.cfg
}

func (f *FrameworkContext) Logger() api.LoggerInterface {
	return f.logger
}

func (f *FrameworkContext) Metrics() api.MetricsInterface {
	return f.metrics
}

func (f *FrameworkContext) Transport() api.TransportInterface {
	return f.transport
}

func (f *FrameworkContext) Storage() api.StorageInterface {
	return f.storage
}

func (f *FrameworkContext) Compliance() api.ComplianceInterface {
	return f.compliance
}

func (f *FrameworkContext) ServiceConfig() *config.ServiceConfig {
	return f.serviceConfig
}

func (f *FrameworkContext) SetService(s api.ServiceInterface) {
	f.service = s
}

func (f *FrameworkContext) Service() api.ServiceInterface {
	return f.service
}

func (f *FrameworkContext) ServiceName() string {
	return f.config.Framework.ServiceName
}

