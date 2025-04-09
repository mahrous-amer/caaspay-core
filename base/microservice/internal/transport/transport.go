package transport

import (
	"context"
	"time"
)

// Transport defines the messaging transport interface.
type Transport interface {
	// Publish sends a one-way message.
	Publish(ctx context.Context, stream string, data []byte) error
	// Request sends an RPC request and waits for a response.
	Request(ctx context.Context, stream string, data []byte, timeout time.Duration) ([]byte, error)
	// Subscribe listens to a stream and dispatches messages to a handler.
	Subscribe(stream string, handler HandlerFunc) error
	// Close cleans up resources and shuts down the transport.
	Close() error
}

// HandlerFunc defines a callback function for processing incoming messages.
type HandlerFunc func(ctx context.Context, request []byte) ([]byte, error)
