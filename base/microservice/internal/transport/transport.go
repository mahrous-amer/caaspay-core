package transport

import (
	"context"
	"time"
)

// Transport defines the messaging transport interface for pluggable broker backends.
type Transport interface {
	// Publish sends a one-way message to the broker (fire-and-forget).
	Publish(ctx context.Context, stream string, data []byte) error

	// Request sends a request and waits for a response on a reply stream.
	Request(ctx context.Context, stream string, data []byte, timeout time.Duration) ([]byte, error)

	// Subscribe listens to a stream and dispatches messages to a handler.
	Subscribe(stream string, handler HandlerFunc) error

	// Close shuts down and cleans up any transport-level resources.
	Close() error

	// IsHealthy checks the health status of the transport connection.
	IsHealthy() bool
}

// HandlerFunc defines a callback for processing incoming messages.
type HandlerFunc func(ctx context.Context, request []byte) ([]byte, error)
