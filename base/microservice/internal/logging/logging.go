package logging

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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
}

// NewLogger initializes a logger instance with framework configuration.
func NewLogger(serviceName string, logLevel string, redact bool) *Logger {
	return &Logger{
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

// LogWithContext logs messages with structured key-value pairs and OpenTelemetry tracing.
func (l *Logger) LogWithContext(ctx context.Context, level, message string, fields map[string]interface{}) {
	if !l.shouldLog(level) {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	// Extract OpenTelemetry trace info
	spanCtx := trace.SpanContextFromContext(ctx)
	traceID := spanCtx.TraceID().String()
	spanID := spanCtx.SpanID().String()

	// Apply redaction if needed
	if l.redact {
		fields = redactSensitiveData(fields)
	}

	// Build log entry
	logEntry := map[string]interface{}{
		"time":        time.Now().Format(time.RFC3339),
		"level":       level,
		"service":     l.serviceName,
		"message":     message,
		"trace_id":    traceID,
		"span_id":     spanID,
		"fields":      fields,
	}

	// Convert log entry to JSON
	logJSON, _ := json.Marshal(logEntry)
	fmt.Println(string(logJSON))

	// Send attributes to OpenTelemetry trace
	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		for key, value := range fields {
			span.SetAttributes(attribute.String(key, fmt.Sprintf("%v", value)))
		}
	}
}

// Info logs informational messages.
func (l *Logger) Info(ctx context.Context, message string, fields map[string]interface{}) {
	l.LogWithContext(ctx, LevelInfo, message, fields)
}

// Debug logs debug messages (only if log level is debug).
func (l *Logger) Debug(ctx context.Context, message string, fields map[string]interface{}) {
	if l.logLevel == LevelDebug {
		l.LogWithContext(ctx, LevelDebug, message, fields)
	}
}

// Error logs error messages.
func (l *Logger) Error(ctx context.Context, message string, fields map[string]interface{}) {
	l.LogWithContext(ctx, LevelError, message, fields)
}

// Fatal logs fatal errors and exits.
func (l *Logger) Fatal(ctx context.Context, message string, fields map[string]interface{}) {
	l.LogWithContext(ctx, LevelFatal, message, fields)
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

// isSensitiveKey checks if a key should be redacted.
func isSensitiveKey(key string) bool {
	sensitiveKeywords := []string{"password", "secret", "token", "apikey"}
	for _, keyword := range sensitiveKeywords {
		if key == keyword {
			return true
		}
	}
	return false
}

// redactSensitiveData replaces sensitive values with a placeholder.
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
