package framework

import (
	"fmt"
	"time"

	"github.com/caaspay/caaspay-core/internal/compliance"
	"github.com/caaspay/caaspay-core/internal/config"
	"github.com/caaspay/caaspay-core/internal/logging"
	"github.com/caaspay/caaspay-core/internal/metrics"
	"github.com/caaspay/caaspay-core/internal/storage"
	"github.com/caaspay/caaspay-core/internal/transport"
)

// FrameworkContext encapsulates all framework components.
type FrameworkContext struct {
	Config        *config.Config
	Logger        *logging.Logger
	Metrics       *metrics.Metrics
	Transport     transport.Transport
	Storage       storage.Store
	Compliance    *compliance.ComplianceReporter
	ServiceName   string
	ServiceConfig *config.ServiceConfig
}

// NewFrameworkContext initializes all framework components and returns a unified context.
func NewFrameworkContext() (*FrameworkContext, error) {
	// Load configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	// Initialize logger
	logger := logging.NewLogger(cfg.Framework.ServiceName, cfg.Framework.Logging.Level, cfg.Framework.Logging.RedactSensitive)

	// Initialize metrics
	metricsInstance, err := metrics.NewMetrics(cfg.Framework.ServiceName, &cfg.Framework.Observability)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize metrics: %w", err)
	}

	// Initialize transport
	redisCfg := transport.RedisTransportConfig{
		RedisAddr:          cfg.Framework.Transport.RedisAddress,
		UseCompression:     cfg.Framework.Transport.UseCompression,
		UseEncryption:      cfg.Framework.Transport.UseEncryption,
		ServiceReplyStream: fmt.Sprintf("%s_reply", cfg.Framework.ServiceName),
		MaxRetries:         3,
		RetryDelay:         500 * time.Millisecond,
	}
	redisTransport := transport.NewRedisTransport(redisCfg, logger)

	// Initialize storage
	store, err := storage.NewStore(cfg.Framework.Storage)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize storage: %w", err)
	}

	// Initialize compliance reporter
	complianceReporter := compliance.NewComplianceReporter(cfg, metricsInstance)

	// Map service-specific configuration
	serviceConfig := &config.ServiceConfig{}
	if serviceData, ok := cfg.Service["example-service"]; ok {
		// Try to unmarshal into ServiceConfig
		if err := config.MapToStruct(serviceData, serviceConfig); err != nil {
			return nil, fmt.Errorf("failed to map service config: %w", err)
		}
	} else {
		// Use default service-specific configuration
		serviceConfig = config.DefaultServiceConfig()
	}

	return &FrameworkContext{
		Config:        cfg,
		Logger:        logger,
		Metrics:       metricsInstance,
		Transport:     redisTransport,
		Storage:       store,
		Compliance:    complianceReporter,
		ServiceName:   cfg.Framework.ServiceName,
		ServiceConfig: serviceConfig,
	}, nil
}
