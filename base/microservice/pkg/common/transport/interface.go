package transport

import (
	"context"
	"time"
)

// HandlerFunc defines a callback for processing incoming messages.
type HandlerFunc func(ctx context.Context, request *TransportMessage) (*TransportMessage, error)

// TransportInterface defines the messaging transport interface for pluggable broker backends.
type TransportInterface interface {
	Publish(ctx context.Context, stream string, data []byte) error
	Request(ctx context.Context, stream string, msg *TransportMessage, timeout time.Duration, caller string) (*TransportMessage, error)
	Subscribe(ctx context.Context, consumerGroup string, stream string, handler HandlerFunc, checkDeadline bool) error
	Emit(ctx context.Context, stream string, msg *TransportMessage) error
	Close() error
	IsHealthy() bool
	CleanupOnShutdown()
	CleanupOnStartup()
}
