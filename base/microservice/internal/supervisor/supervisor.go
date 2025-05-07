package supervisor

import (
	"context"
	"sync"

	"github.com/caaspay/caaspay-core/internal/logging"
)

// Supervisor manages controlled goroutines with centralized error handling and graceful shutdown.
type Supervisor struct {
	wg      sync.WaitGroup
	errChan chan error
	ctx     context.Context
	cancel  context.CancelFunc
	logger  *logging.Logger
	doneCh  chan struct{}
	once    sync.Once
	active  sync.Map
}

// NewSupervisor initializes a new Supervisor instance.
func NewSupervisor(ctx context.Context, cancel context.CancelFunc, logger *logging.Logger) *Supervisor {
	s := &Supervisor{
		errChan: make(chan error, 1),
		ctx:     ctx,
		cancel:  cancel,
		logger:  logger,
		doneCh:  make(chan struct{}),
	}
	return s
}

// Go launches a managed goroutine. If the function returns an error, it's reported and shutdown begins.
func (s *Supervisor) Go(name string, fn func(ctx context.Context) error) {
	s.wg.Add(1)
	s.active.Store(name, true)

	go func() {
		defer func() {
			s.active.Delete(name)
			s.logger.Info("Supervisor goroutine finished", map[string]interface{}{"name": name})
			s.wg.Done()
		}()

		if err := fn(s.ctx); err != nil {
			select {
			case s.errChan <- err:
			default:
			}
			s.logger.Error("Supervisor goroutine crashed", map[string]interface{}{
				"name":  name,
				"error": err.Error(),
			})
		}
	}()

	s.logger.Info("✅ Supervisor started goroutine", map[string]interface{}{"name": name})
}

// GoLoop runs fn repeatedly until ctx is done.
// If fn returns an error, the error is sent to errChan to trigger shutdown.
func (s *Supervisor) GoLoop(name string, fn func(ctx context.Context) error) {
	s.wg.Add(1)
	s.active.Store(name, true)

	go func() {
		defer func() {
			s.active.Delete(name)
			s.logger.Info("Supervisor loop finished", map[string]interface{}{"name": name})
			s.wg.Done()
		}()

		for {
			select {
			case <-s.ctx.Done():
				s.logger.Info("Supervisor loop exiting (ctx cancelled)", map[string]interface{}{"name": name})
				return
			default:
				if err := fn(s.ctx); err != nil {
					s.logger.Error("Supervisor loop error, triggering shutdown", map[string]interface{}{
						"name":  name,
						"error": err.Error(),
					})
					// trigger shutdown by sending to errChan
					select {
					case s.errChan <- err:
					default:
					}
					return // exit loop after triggering shutdown
				}
			}
		}
	}()

	s.logger.Info("🔁 Supervisor started loop", map[string]interface{}{"name": name})
}

// WaitAndShutdown blocks until an error or shutdown signal occurs, then runs the provided shutdown logic.
func (s *Supervisor) WaitAndShutdown(onShutdown func()) {
	select {
	case <-s.ctx.Done():
		// graceful shutdown initiated (e.g. SIGINT)
	case <-s.errChan:
		// an error-triggered shutdown
	}

	onShutdown() // ✅ must always run regardless of reason
	s.logger.Info("📋 Waiting for goroutines to finish:", nil)
	s.active.Range(func(key, value any) bool {
		s.logger.Info("🕒 Still active:", map[string]interface{}{"name": key})
		return true
	})
	s.wg.Wait()

	s.once.Do(func() {
		close(s.doneCh)
	})
}

// Shutdown signals all running goroutines to stop.
func (s *Supervisor) Shutdown() {
	s.cancel()
}

// Done returns a channel that's closed when all goroutines have exited.
func (s *Supervisor) Done() <-chan struct{} {
	return s.doneCh
}

// StopAll stops all routines and waits for them to exit.
func (s *Supervisor) StopAll() {
	s.Shutdown()
	s.wg.Wait()
}
