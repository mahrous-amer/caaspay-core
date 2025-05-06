package logging

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Logger provides structured logging with redaction and context.
type Logger struct {
	serviceName string
	logLevel    string
	redact      bool
	mu          sync.Mutex
	tracer      trace.Tracer
	ctx         context.Context
}

// NewLogger initializes a logger instance with framework configuration and context.
func NewLogger(ctx context.Context, serviceName string, logLevel string, redact bool) *Logger {
	return &Logger{
		ctx:         ctx,
		serviceName: serviceName,
		logLevel:    logLevel,
		redact:      redact,
		tracer:      otel.Tracer(serviceName),
	}
}

// Log levels for structured logging
const (
	LevelTrace = "TRACE"
	LevelDebug = "DEBUG"
	LevelInfo  = "INFO"
	LevelWarn  = "WARN"
	LevelError = "ERROR"
	LevelFatal = "FATAL"
)

// Log logs messages with structured key-value pairs and OpenTelemetry tracing.
func (l *Logger) Log(level, message string, fields map[string]interface{}) {
	if !l.shouldLog(level) {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	spanCtx := trace.SpanContextFromContext(l.ctx)
	traceID := spanCtx.TraceID().String()
	spanID := spanCtx.SpanID().String()

	if l.redact {
		fields = redactSensitiveData(fields)
	}

	logEntry := map[string]interface{}{
		"time":     time.Now().Format(time.RFC3339),
		"level":    level,
		"service":  l.serviceName,
		"message":  message,
		"trace_id": traceID,
		"span_id":  spanID,
		"fields":   fields,
	}

	logJSON, _ := json.Marshal(logEntry)
	fmt.Println(string(logJSON))

	if span := trace.SpanFromContext(l.ctx); span.IsRecording() {
		for key, value := range fields {
			span.SetAttributes(attribute.String(key, fmt.Sprintf("%v", value)))
		}
	}
}

// Info logs informational messages.
func (l *Logger) Info(message string, fields map[string]interface{}) {
	l.Log(LevelInfo, message, fields)
}

// Debug logs debug messages.
func (l *Logger) Debug(message string, fields map[string]interface{}) {
	if l.logLevel == LevelDebug {
		l.Log(LevelDebug, message, fields)
	}
}

// Warn logs warning messages.
func (l *Logger) Warn(message string, fields map[string]interface{}) {
	l.Log(LevelWarn, message, fields)
}

// Error logs error messages.
func (l *Logger) Error(message string, fields map[string]interface{}) {
	l.Log(LevelError, message, fields)
}

// Fatal logs fatal errors and exits.
func (l *Logger) Fatal(message string, fields map[string]interface{}) {
	l.Log(LevelFatal, message, fields)
	os.Exit(1)
}

// shouldLog checks if the current log level allows logging the given level.
func (l *Logger) shouldLog(level string) bool {
	allowedLevels := map[string]int{
		LevelTrace: 1,
		LevelDebug: 2,
		LevelInfo:  3,
		LevelWarn:  4,
		LevelError: 5,
		LevelFatal: 6,
	}

	currentLevel := allowedLevels[l.logLevel]
	targetLevel := allowedLevels[level]

	return targetLevel >= currentLevel
}

func isSensitiveKey(key string) bool {
	sensitiveKeywords := []string{"password", "secret", "token", "apikey"}
	for _, keyword := range sensitiveKeywords {
		if key == keyword {
			return true
		}
	}
	return false
}

func redactSensitiveData(data map[string]interface{}) map[string]interface{} {
	redacted := make(map[string]interface{})
	for key, value := range data {
		if isSensitiveKey(key) {
			redacted[key] = "[REDACTED]"
		} else {
			redacted[key] = value
		}
	}
	return redacted
}
