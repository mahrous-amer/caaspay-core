package compliance

import (
	"context"
	"fmt"
	"time"

	"github.com/caaspay/caaspay-core/pkg/api"
	"github.com/caaspay/caaspay-core/pkg/common/logger"
	"github.com/caaspay/caaspay-core/pkg/common/metrics"
	"github.com/caaspay/caaspay-core/pkg/common/tracing"
)

// ComplianceReporter handles compliance event tracking and metrics.
type ComplianceReporter struct {
	ctx         context.Context
	log         *logger.Logger
	metrics     *metrics.Metrics
	tracer      *tracing.TracerManager
	enabled     bool
	appName     string
	environment string
}

// NewComplianceReporter initializes compliance tracking using config settings.
func NewComplianceReporter(ctx context.Context, cfg *api.Config, log *logger.Logger, metrics *metrics.Metrics, tracer *tracing.TracerManager) *ComplianceReporter {
	return &ComplianceReporter{
		ctx:         ctx,
		log:         log,
		metrics:     metrics,
		tracer:      tracer,
		enabled:     cfg.ComplianceEnabled,
		appName:     cfg.AppName,
		environment: cfg.Env,
	}
}

// TrackEvent logs a compliance event along with tracing and metrics.
func (cr *ComplianceReporter) TrackEvent(eventName string) {
	if !cr.enabled {
		return
	}

	_, span := cr.tracer.StartSpan(eventName)
	defer span.End()

	cr.log.Info(cr.ctx, "🔍 Compliance Event", map[string]interface{}{
		"event":   eventName,
		"traceID": span.SpanContext().TraceID().String(),
		"app":     cr.appName,
		"env":     cr.environment,
	})

	cr.metrics.Increment(eventName)
	cr.metrics.RecordTiming(eventName, 1*time.Millisecond) // Default 1ms for event-based metrics
}

// ComplianceChecker verifies that PCI and license compliance rules are met.
type ComplianceChecker struct {
	cfg      *api.Config
	reporter *ComplianceReporter
}

// NewComplianceChecker creates a new checker.
func NewComplianceChecker(cfg *api.Config, reporter *ComplianceReporter) *ComplianceChecker {
	return &ComplianceChecker{
		cfg:      cfg,
		reporter: reporter,
	}
}

// ValidatePCICompliance performs checks to ensure PCI compliance.
func (cc *ComplianceChecker) ValidatePCICompliance() error {
	start := time.Now()

	if !cc.cfg.PCIEnabled {
		return fmt.Errorf("PCI compliance not enabled")
	}
	if cc.cfg.EncryptionKey == "" {
		return fmt.Errorf("encryption key not configured")
	}

	duration := time.Since(start)
	cc.reporter.metrics.RecordTiming("compliance.pci_check_duration", duration)
	cc.reporter.TrackEvent("compliance.pci_validated")

	return nil
}
