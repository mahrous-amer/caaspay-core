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
	metrics       api.MetricsInterface
	transport     api.TransportInterface
	storage       api.StorageInterface
	compliance    api.ComplianceInterface
	config        *config.Config
	serviceConfig *config.ServiceConfig
	serviceName   string
	validator     api.ValidatorInterface
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
		ReadTimeout:        cfg.Framework.Transport.ReadTimeout,
		WriteTimeout:       cfg.Framework.Transport.WriteTimeout,
		StreamReadCount:    cfg.Framework.Transport.StreamReadCount,
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

	ctx := &FrameworkContext{
		logger:        logger,
		metrics:       metricsInstance,
		transport:     redisTransport,
		storage:       store,
		compliance:    complianceReporter,
		config:        cfg,
		serviceConfig: serviceConfig,
		serviceName:   cfg.Framework.ServiceName,
		validator:     validator,
	}

	if cfg.Framework.HealthCheck.HeartbeatEnabled {
		go startHeartbeat(ctx)
	}

	return ctx, nil
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
func (f *FrameworkContext) SetService(s api.ServiceInterface)    { f.service = s }
func (f *FrameworkContext) Service() api.ServiceInterface        { return f.service }

func (f *FrameworkContext) IsHealthy() bool {
	ctx := context.Background()
	healthy := true

	if !f.transport.IsHealthy() {
		f.logger.Error(ctx, "🚨 Transport not healthy", nil)
		healthy = false
	}

	if f.service != nil {
		if err := f.service.HealthCheck(ctx); err != nil {
			f.logger.Error(ctx, "🚨 Service health check failed", map[string]interface{}{"error": err.Error()})
			healthy = false
		}
	}

	return healthy
}

func startHeartbeat(f *FrameworkContext) {
	ticker := time.NewTicker(f.config.Framework.HealthCheck.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ctx := context.Background()
			if f.IsHealthy() {
				f.logger.Info(ctx, "💓 Framework heartbeat... all systems healthy", nil)
			} else {
				f.logger.Error(ctx, "💔 Framework heartbeat... one or more systems unhealthy", nil)
			}
		}
	}
}
