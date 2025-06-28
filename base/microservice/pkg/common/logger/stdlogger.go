package logger

import (
	"context"
	"log"
)

type StdLogger struct {
	fatalSignal chan struct{}
}

func NewStdLogger() *StdLogger {
	return &StdLogger{fatalSignal: make(chan struct{})}
}

func (l *StdLogger) Info(_ context.Context, msg string, fields map[string]interface{}) {
	log.Printf("[INFO] %s %+v\n", msg, fields)
}
func (l *StdLogger) Error(_ context.Context, msg string, fields map[string]interface{}) {
	log.Printf("[ERROR] %s %+v\n", msg, fields)
}
func (l *StdLogger) Warn(_ context.Context, msg string, fields map[string]interface{}) {
	log.Printf("[WARN] %s %+v\n", msg, fields)
}
func (l *StdLogger) Debug(_ context.Context, msg string, fields map[string]interface{}) {
	log.Printf("[DEBUG] %s %+v\n", msg, fields)
}
func (l *StdLogger) Fatal(_ context.Context, msg string, fields map[string]interface{}) {
	log.Printf("[FATAL] %s %+v\n", msg, fields)
	close(l.fatalSignal)
}
func (l *StdLogger) FatalSignal() <-chan struct{} {
	return l.fatalSignal
}
