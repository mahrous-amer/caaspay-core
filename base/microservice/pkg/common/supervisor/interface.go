package supervisor

import "context"
import "time"

// SupervisorInterface defines how the framework manages goroutines.
type SupervisorInterface interface {
	// Go launches a managed goroutine that can report failure.
	Go(name string, fn func(ctx context.Context) error)
	GoLoop(name string, fn func(ctx context.Context) (time.Duration, error))

	// WaitAndShutdown blocks until an error or shutdown occurs, and runs shutdown logic.
	WaitAndShutdown(onShutdown func())

	// Shutdown triggers graceful cancellation of all supervised goroutines.
	Shutdown()

	// Done returns a channel closed once all goroutines have exited.
	Done() <-chan struct{}

	// StopAll is optionally used for forced termination or post-processing logic.
	StopAll()
	ErrorChannel() <-chan error
}
