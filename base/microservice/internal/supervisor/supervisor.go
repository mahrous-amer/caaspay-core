package supervisor

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/caaspay/caaspay-core/internal/logging"
	"github.com/google/uuid"
)

const minLoopDelay = 10 * time.Millisecond

// goroutineInfo tracks metadata about each managed goroutine.
type goroutineInfo struct {
	StartTime  time.Time
	Origin     string // e.g. "framework", "service"
	CallerFile string // source file where it was started (best-effort)
	CallerLine int    // line number where it was started (best-effort)
	MethodName string // Method name
}

// Supervisor manages controlled goroutines with centralized error handling and graceful shutdown.
type Supervisor struct {
	wg      sync.WaitGroup
	errChan chan error
	ctx     context.Context
	cancel  context.CancelFunc
	logger  *logging.Logger
	doneCh  chan struct{}
	once    sync.Once
	active  sync.Map // map[string]goroutineInfo
}

// NewSupervisor initializes a new Supervisor instance.
func NewSupervisor(ctx context.Context, cancel context.CancelFunc, logger *logging.Logger) *Supervisor {
	return &Supervisor{
		errChan: make(chan error, 1),
		ctx:     ctx,
		cancel:  cancel,
		logger:  logger,
		doneCh:  make(chan struct{}),
	}
}

func (s *Supervisor) storeMetadata(name, origin string) string {
	id := uuid.New().String()
	finalName := name + ":" + id
	file, line := callerInfo(3)
	s.active.Store(finalName, goroutineInfo{
		StartTime:  time.Now(),
		Origin:     origin,
		CallerFile: file,
		CallerLine: line,
		MethodName: name,
	})
	return finalName
}

func callerInfo(skip int) (string, int) {
	if _, file, line, ok := runtime.Caller(skip); ok {
		return file, line
	}
	return "unknown", 0
}

// Go launches a managed goroutine.
func (s *Supervisor) Go(name string, fn func(ctx context.Context) error) {
	s.wg.Add(1)
	finalName := s.storeMetadata(name, "framework")

	go func() {
		defer func() {
			s.active.Delete(finalName)
			s.logger.Info("Supervisor goroutine finished", map[string]interface{}{"name": finalName})
			s.wg.Done()
		}()

		if err := fn(s.ctx); err != nil {
			select {
			case s.errChan <- err:
			default:
			}
			s.logger.Error("Supervisor goroutine crashed", map[string]interface{}{
				"name":  finalName,
				"error": err.Error(),
			})
		}
	}()

	s.logger.Info("✅ Supervisor started goroutine", map[string]interface{}{"name": finalName})
}

// GoLoop runs fn repeatedly until ctx is done.
func (s *Supervisor) GoLoop(name string, interval time.Duration, fn func(ctx context.Context) error) {
	s.wg.Add(1)
	finalName := s.storeMetadata(name, "framework")

	go func() {
		defer func() {
			s.active.Delete(finalName)
			s.logger.Info("Supervisor loop finished", map[string]interface{}{"name": finalName})
			s.wg.Done()
		}()

		for {
			select {
			case <-s.ctx.Done():
				s.logger.Info("Supervisor loop exiting (ctx cancelled)", map[string]interface{}{"name": finalName})
				return
			default:
				start := time.Now()

				if err := fn(s.ctx); err != nil {
					s.logger.Error("Supervisor loop error, triggering shutdown", map[string]interface{}{
						"name":  finalName,
						"error": err.Error(),
					})
					select {
					case s.errChan <- err:
					default:
					}
					return
				}

				// enforce pacing
				if interval == 0 {
					time.Sleep(minLoopDelay)
				} else {
					if interval < minLoopDelay {
						interval = minLoopDelay
					}
					elapsed := time.Since(start)
					if sleep := interval - elapsed; sleep > 0 {
						time.Sleep(sleep)
					}
				}
			}
		}
	}()

	s.logger.Info("🔁 Supervisor started loop", map[string]interface{}{
		"name":     finalName,
		"interval": interval,
	})
}

// WaitAndShutdown blocks until an error or shutdown signal occurs.
func (s *Supervisor) WaitAndShutdown(onShutdown func()) {
	select {
	case <-s.ctx.Done():
	case <-s.errChan:
	}

	onShutdown()
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

// IsHealthy checks for stuck goroutines and emits metrics.
func (s *Supervisor) IsHealthy() bool {
	healthy := true
	now := time.Now()
	activeCount := 0

	s.active.Range(func(key, value any) bool {
		name := key.(string)
		info := value.(goroutineInfo)
		uptime := now.Sub(info.StartTime)
		activeCount++

		entry := map[string]interface{}{
			"name":        name,
			"uptime":      uptime.String(),
			"origin":      info.Origin,
			"caller_file": info.CallerFile,
			"caller_line": info.CallerLine,
			"method_name": info.MethodName,
		}

		if uptime > time.Minute {
			s.logger.Warn("⚠️ Long-running goroutine", entry)
			//healthy = false
		} else {
			s.logger.Info("🧵 Active goroutine", entry)
		}
		return true
	})

	s.logger.Info("📊 Supervisor active goroutines", map[string]interface{}{
		"count": activeCount,
	})

	return healthy
}
