package logger

import "context"

// --- Logger ---
type LoggerInterface interface {
	Info(ctx context.Context, message string, fields map[string]interface{})
	Error(ctx context.Context, message string, fields map[string]interface{})
	Warn(ctx context.Context, message string, fields map[string]interface{})
	Debug(ctx context.Context, message string, fields map[string]interface{})
	Fatal(ctx context.Context, message string, fields map[string]interface{})
	FatalSignal() <-chan struct{}
}
