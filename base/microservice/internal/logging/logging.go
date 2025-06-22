package logging

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

type Logger struct {
	slog        *slog.Logger
	logLevel    slog.Level
	redact      bool
	serviceName string
	fatalSignal chan struct{}
}

// NewLogger initializes the structured logger.
func NewLogger(serviceName string, level string, redact bool) *Logger {
	levelMap := map[string]slog.Level{
		"DEBUG": slog.LevelDebug,
		"INFO":  slog.LevelInfo,
		"WARN":  slog.LevelWarn,
		"ERROR": slog.LevelError,
	}

	logLevel, ok := levelMap[strings.ToUpper(level)]
	if !ok {
		logLevel = slog.LevelInfo
	}

	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	})
	enhanced := &contextAwareHandler{
		next:        handler,
		serviceName: serviceName,
	}

	return &Logger{
		slog:        slog.New(enhanced),
		logLevel:    logLevel,
		redact:      redact,
		serviceName: serviceName,
		fatalSignal: make(chan struct{}, 1),
	}
}

func (l *Logger) FatalSignal() <-chan struct{} {
	return l.fatalSignal
}

func (l *Logger) Debug(ctx context.Context, msg string, fields map[string]interface{}) {
	l.slog.DebugContext(ctx, msg, mapToArgs(l.redact, fields)...)
}

func (l *Logger) Info(ctx context.Context, msg string, fields map[string]interface{}) {
	l.slog.InfoContext(ctx, msg, mapToArgs(l.redact, fields)...)
}

func (l *Logger) Warn(ctx context.Context, msg string, fields map[string]interface{}) {
	l.slog.WarnContext(ctx, msg, mapToArgs(l.redact, fields)...)
}

func (l *Logger) Error(ctx context.Context, msg string, fields map[string]interface{}) {
	l.slog.ErrorContext(ctx, msg, mapToArgs(l.redact, fields)...)
}

func (l *Logger) Fatal(ctx context.Context, msg string, fields map[string]interface{}) {
	l.slog.ErrorContext(ctx, "🛑 FATAL: triggering graceful shutdown", mapToArgs(l.redact, fields)...)

	// non-blocking signal
	select {
	case l.fatalSignal <- struct{}{}:
	default:
		// already signaled
	}
}

func isSensitiveKey(key string) bool {
	sensitive := []string{"password", "secret", "token", "apikey"}
	key = strings.ToLower(key)
	for _, s := range sensitive {
		if strings.Contains(key, s) {
			return true
		}
	}
	return false
}

func redactSensitiveData(data map[string]interface{}) map[string]interface{} {
	redacted := make(map[string]interface{}, len(data))
	for k, v := range data {
		if isSensitiveKey(k) {
			redacted[k] = "[REDACTED]"
		} else {
			redacted[k] = v
		}
	}
	return redacted
}

func mapToArgs(redact bool, fields map[string]interface{}) []any {
	if fields == nil {
		return nil
	}
	if redact {
		fields = redactSensitiveData(fields)
	}
	args := make([]any, 0, len(fields)*2)
	for k, v := range fields {
		args = append(args, k, v)
	}
	return args
}
