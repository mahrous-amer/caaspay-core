package framework

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"time"

	"github.com/caaspay/caaspay-core/internal/compliance"
	"github.com/caaspay/caaspay-core/internal/config"
	"github.com/caaspay/caaspay-core/internal/httpclient"
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
	httpClient    api.HTTPClientInterface
}

// NewFrameworkContext initializes all framework components and returns a unified context.
func NewFrameworkContext(rootCtx context.Context) (*FrameworkContext, error) {
	cfg, err := config.LoadConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	ctx, cancel := context.WithCancel(rootCtx)

	logger := logging.NewLogger(cfg.Framework.ServiceName, cfg.Framework.Logging.Level, cfg.Framework.Logging.RedactSensitive)

	metricsInstance, err := metrics.NewMetrics(ctx, cfg.Framework.ServiceName, &cfg.Framework.Observability, logger)
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
		ResponseStreamSubOnce:   cfg.Framework.Transport.ResponseStreamSubOnce,
		MaxRetries:              cfg.Framework.Transport.MaxRetries,
		RetryDelay:              cfg.Framework.Transport.RetryDelay,
		ReadTimeout:             cfg.Framework.Transport.ReadTimeout,
		WriteTimeout:            cfg.Framework.Transport.WriteTimeout,
		StreamReadCount:         cfg.Framework.Transport.StreamReadCount,
		StreamTrimMaxLen:        cfg.Framework.Transport.StreamTrimMaxLen,
		StreamTrimApprox:        cfg.Framework.Transport.StreamTrimApprox,
		PeriodicTrimFreq:        cfg.Framework.Transport.PeriodicTrimFreq,
		MoveExpiredToDLQ:        cfg.Framework.Transport.MoveExpiredToDLQ,
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

	httpClientCfg := httpclient.Config{
		Timeout:    cfg.Framework.HTTPClient.Timeout,
		UserAgent:  cfg.Framework.HTTPClient.UserAgent,
		Logger:     logger,
		Metrics:    metricsInstance,
		Supervisor: supervisorInstance,
		Compliance: complianceReporter,
	}
	httpClient := httpclient.NewClient(httpClientCfg)

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
		httpClient:    httpClient,
	}

	if cfg.Framework.HealthCheck.HeartbeatEnabled {
		fwCtx.supervisor.GoLoop("framework_heartbeat", func(ctx context.Context) (time.Duration, error) {
			if fwCtx.IsHealthy() {
				fwCtx.logger.Info(ctx, "💓 Framework heartbeat... all systems healthy", nil)
			} else {
				fwCtx.logger.Error(ctx, "💔 Framework heartbeat... one or more systems unhealthy", nil)
			}
			return cfg.Framework.HealthCheck.HeartbeatInterval, nil
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

//func (f *FrameworkContext) Shutdown(ctx context.Context)         { f.service.Shutdown(ctx) }

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
		f.logger.Error(f.ctx, "🚨 Transport not healthy", nil)
		healthy = false
	}

	if f.service != nil {
		if err := f.service.HealthCheck(f.ctx); err != nil {
			f.logger.Error(f.ctx, "🚨 Service health check failed", map[string]interface{}{"error": err.Error()})
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
	start := time.Now()

	var timing = struct {
		validate  time.Duration
		encode    time.Duration
		transport time.Duration
		decode    time.Duration
	}{}

	// 1. Validate input
	if validator := f.Validator(); validator != nil {
		vstart := time.Now()
		if err := validator.ValidateStruct(input); err != nil {
			return fmt.Errorf("input validation failed: %w", err)
		}
		timing.validate = time.Since(vstart)
	}

	// 2. Marshal input
	estart := time.Now()
	rawArgs, err := json.Marshal(input)
	if err != nil {
		return fmt.Errorf("failed to marshal input: %w", err)
	}
	timing.encode = time.Since(estart)

	// 3. Prepare and send request
	msg := api.NewTransportMessage(f.ServiceName(), stream, rawArgs, timeout)
	tstart := time.Now()

	_, _, line, ok := runtime.Caller(1) // 1 = skip current function
	caller := "unknown"
	if ok {
		caller = fmt.Sprintf("%d", line)
	}
	respMsg, err := f.Transport().Request(f.ctx, stream, msg, timeout, caller)
	timing.transport = time.Since(tstart)

	if err != nil {
		return fmt.Errorf("transport request failed: %w", err)
	}

	//// 4. Decode response
	dstart := time.Now()
	//respMsg, err := api.DecodeTransportMessage(respBytes)
	//if err != nil {
	//	return fmt.Errorf("failed to decode TransportMessage: %w", err)
	//}
	if err := json.Unmarshal(respMsg.Response, output); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}
	timing.decode = time.Since(dstart)

	// 5. Optional: log or record metrics
	total := time.Since(start)
	f.Metrics().ObserveHistogram("framework.rpc.total", total.Seconds(), "stream", stream)
	f.Metrics().ObserveHistogram("framework.rpc.encode", timing.encode.Seconds(), "stream", stream)
	f.Metrics().ObserveHistogram("framework.rpc.transport", timing.transport.Seconds(), "stream", stream)
	//f.Metrics().ObserveHistogram("framework.rpc.decode", timing.decode.Seconds(), "stream", stream)

	f.Logger().Info(f.ctx, "⏱️ RPC Request Timings", map[string]interface{}{
		"stream":       stream,
		"validate_ms":  timing.validate.Milliseconds(),
		"encode_ms":    timing.encode.Milliseconds(),
		"transport_ms": timing.transport.Milliseconds(),
		//		"decode_ms":    timing.decode.Milliseconds(),
		"total_ms": total.Milliseconds(),
	})

	return nil
}

//func (f *FrameworkContext) RequestHTTP(ctx context.Context, method, url string, body []byte, headers map[string]string) (*http.Response, error) {
//	return f.httpClient.Request(ctx, method, url, body, headers)
//}

func (f *FrameworkContext) RequestHTTPRaw(
	ctx context.Context,
	method string,
	url string,
	body []byte,
	headers map[string]string,
) (*http.Response, []byte, error) {
	return f.httpClient.RequestHTTPRaw(ctx, method, url, body, headers)
}

func (f *FrameworkContext) RequestHTTP(
	ctx context.Context,
	method string,
	url string,
	input any,
	headers map[string]string,
	output any,
) error {
	return f.httpClient.RequestHTTP(ctx, method, url, input, headers, output)
}
