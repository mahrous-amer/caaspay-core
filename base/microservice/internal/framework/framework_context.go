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
	"github.com/caaspay/caaspay-core/internal/validation"
	"github.com/caaspay/caaspay-core/pkg/api"
)

// FrameworkContext encapsulates all framework components.
type FrameworkContext struct {
	logger        api.LoggerInterface
	metrics       *metrics.Metrics
	transport     transport.Transport
	storage       storage.Store
	compliance    api.ComplianceReporterInterface
	config        *config.Config
	serviceConfig *config.ServiceConfig
	serviceName   string
	validator     api.ValidatorInterface
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
	validator := validation.NewValidator()

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
		logger:        logger,
		metrics:       metricsInstance,
		transport:     redisTransport,
		storage:       store,
		compliance:    complianceReporter,
		config:        cfg,
		serviceConfig: serviceConfig,
		serviceName:   cfg.Framework.ServiceName,
		validator:     validator,
	}, nil
}

func (f *FrameworkContext) Logger() api.LoggerInterface          { return f.logger }
func (f *FrameworkContext) Metrics() api.MetricsInterface        { return f.metrics }
func (f *FrameworkContext) Transport() api.TransportInterface    { return f.transport }
func (f *FrameworkContext) Storage() api.StorageInterface        { return f.storage }
func (f *FrameworkContext) Compliance() api.ComplianceInterface  { return f.compliance }
func (f *FrameworkContext) Config() *config.Config               { return f.config }
func (f *FrameworkContext) ServiceConfig() *config.ServiceConfig { return f.serviceConfig }
func (f *FrameworkContext) ServiceName() string                  { return f.serviceName }
func (f *FrameworkContext) Validator() api.ValidatorInterface    { return f.validator }

func (f *FrameworkContext) IsHealthy() bool {
	if r, ok := f.Transport().(interface{ IsHealthy() bool }); ok {
		if !r.IsHealthy() {
			f.Logger().Error(context.Background(), "🚨 Transport not healthy", nil)
			return false
		}
	}
	return true
}

func startHeartbeat(logger api.LoggerInterface, transport api.TransportInterface, interval time.Duration) {
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
