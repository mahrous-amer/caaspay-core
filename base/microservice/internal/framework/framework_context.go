package framework

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/caaspay/caaspay-core/internal/compliance"
	"github.com/caaspay/caaspay-core/internal/config"
	"github.com/caaspay/caaspay-core/internal/logging"
	"github.com/caaspay/caaspay-core/internal/metrics"
	"github.com/caaspay/caaspay-core/internal/storage"
	"github.com/caaspay/caaspay-core/internal/supervisor"
	"github.com/caaspay/caaspay-core/internal/tracing"
	"github.com/caaspay/caaspay-core/internal/transport"
	"github.com/caaspay/caaspay-core/internal/validation"
	"github.com/caaspay/caaspay-core/pkg/api"
)

// FrameworkContext encapsulates all framework components.
type FrameworkContext struct {
	ctx           context.Context
	cancel        context.CancelFunc
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
	supervisor    api.SupervisorInterface
}

// NewFrameworkContext initializes all framework components and returns a unified context.
func NewFrameworkContext(rootCtx context.Context) (*FrameworkContext, error) {
	cfg, err := config.LoadConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	ctx, cancel := context.WithCancel(rootCtx)

	logger := logging.NewLogger(ctx, cfg.Framework.ServiceName, cfg.Framework.Logging.Level, cfg.Framework.Logging.RedactSensitive)

	metricsInstance, err := metrics.NewMetrics(ctx, cfg.Framework.ServiceName, &cfg.Framework.Observability)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize metrics: %w", err)
	}

	tracerManager, err := tracing.NewTracerManager(ctx, cfg.Framework.ServiceName, &cfg.Framework.Observability, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize tracer manager: %w", err)
	}

	supervisorInstance := supervisor.NewSupervisor(ctx, cancel, logger)

	redisCfg := transport.RedisTransportConfig{
		RedisAddr:               cfg.Framework.Transport.RedisAddr,
		ServiceInstanceID:       cfg.Framework.InstanceID,
		UseCompression:          cfg.Framework.Transport.UseCompression,
		UseEncryption:           cfg.Framework.Transport.UseEncryption,
		EncryptionKey:           cfg.Framework.Transport.EncryptionKey,
		ServiceReplyStream:      fmt.Sprintf("%s_reply", cfg.Framework.ServiceName),
		ResponseOnServiceStream: cfg.Framework.Transport.ResponseOnServiceStream,
		MaxRetries:              cfg.Framework.Transport.MaxRetries,
		RetryDelay:              cfg.Framework.Transport.RetryDelay,
		ReadTimeout:             cfg.Framework.Transport.ReadTimeout,
		WriteTimeout:            cfg.Framework.Transport.WriteTimeout,
		StreamReadCount:         cfg.Framework.Transport.StreamReadCount,
		StreamTrimMaxLen:        cfg.Framework.Transport.StreamTrimMaxLen,
		StreamTrimApprox:        cfg.Framework.Transport.StreamTrimApprox,
		PeriodicTrimFreq:        cfg.Framework.Transport.PeriodicTrimFreq,
	}

	redisTransport, err := transport.NewRedisTransport(
		ctx,
		logger,
		metricsInstance,
		supervisorInstance,
		redisCfg,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize transport: %w", err)
	}

	store, err := storage.NewStore(cfg.Framework.Storage)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize storage: %w", err)
	}

	complianceReporter := compliance.NewComplianceReporter(ctx, cfg, logger, metricsInstance, tracerManager)
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

	fwCtx := &FrameworkContext{
		ctx:           ctx,
		cancel:        cancel,
		logger:        logger,
		metrics:       metricsInstance,
		transport:     redisTransport,
		storage:       store,
		compliance:    complianceReporter,
		config:        cfg,
		serviceConfig: serviceConfig,
		serviceName:   cfg.Framework.ServiceName,
		validator:     validator,
		supervisor:    supervisorInstance,
	}

	if cfg.Framework.HealthCheck.HeartbeatEnabled {
		fwCtx.supervisor.Go("framework_heartbeat", func(ctx context.Context) error {
			ticker := time.NewTicker(cfg.Framework.HealthCheck.HeartbeatInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return nil
				case <-ticker.C:
					if fwCtx.IsHealthy() {
						fwCtx.logger.Info("💓 Framework heartbeat... all systems healthy", nil)
					} else {
						fwCtx.logger.Error("💔 Framework heartbeat... one or more systems unhealthy", nil)
					}
				}
			}
		})
	}

	return fwCtx, nil
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
func (f *FrameworkContext) Supervisor() api.SupervisorInterface  { return f.supervisor }
func (f *FrameworkContext) Context() context.Context             { return f.ctx }
func (f *FrameworkContext) SetService(s api.ServiceInterface)    { f.service = s }
func (f *FrameworkContext) Service() api.ServiceInterface        { return f.service }

func (f *FrameworkContext) BuildStreamName(kind api.StreamType, service, method string) string {
	return transport.BuildStreamName(api.StreamConfig{
		Type:    kind,
		Service: service,
		Method:  method,
		// Optional: add InstanceID from config if enabled
		//InstanceID: f.config.Framework.InstanceID,
	})
}

func (f *FrameworkContext) BuildRPCStreamName(method string, serviceName ...string) string {
	service := f.serviceName
	if len(serviceName) > 0 && serviceName[0] != "" {
		service = serviceName[0]
	}
	return f.BuildStreamName(api.StreamTypeRPC, service, method)
}

func (f *FrameworkContext) IsHealthy() bool {
	healthy := true

	if !f.transport.IsHealthy() {
		f.logger.Error("🚨 Transport not healthy", nil)
		healthy = false
	}

	if f.service != nil {
		if err := f.service.HealthCheck(); err != nil {
			f.logger.Error("🚨 Service health check failed", map[string]interface{}{"error": err.Error()})
			healthy = false
		}
	}

	if f.supervisor != nil {
		if !f.supervisor.(*supervisor.Supervisor).IsHealthy() {
			healthy = false
		}
	}

	return healthy
}

func (f *FrameworkContext) RequestRPC(stream string, input any, output any, timeout time.Duration) error {
	if validator := f.Validator(); validator != nil {
		if err := validator.ValidateStruct(input); err != nil {
			return fmt.Errorf("input validation failed: %w", err)
		}
	}

	rawArgs, err := json.Marshal(input)
	if err != nil {
		return fmt.Errorf("failed to marshal input: %w", err)
	}

	msg := api.NewTransportMessage(f.ServiceName(), stream, rawArgs)
	// msg.Auth = f.AuthContext()
	// msg.Context = f.RequestContext()

	respBytes, err := f.Transport().Request(stream, msg, timeout)
	if err != nil {
		return fmt.Errorf("transport request failed: %w", err)
	}

	respMsg, err := api.DecodeTransportMessage(respBytes)
	if err != nil {
		return fmt.Errorf("failed to decode TransportMessage: %w", err)
	}

	if err := json.Unmarshal(respMsg.Response, output); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	return nil
}
