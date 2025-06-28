package supervisor

import (
	"context"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/caaspay/caaspay-core/pkg/common/logger"
	"github.com/google/uuid"
)

const minLoopDelay = 10 * time.Millisecond
const shutdownTimeout = 40 * time.Second

// goroutineInfo tracks metadata about each managed goroutine.
type goroutineInfo struct {
	StartTime  time.Time
	Origin     string
	CallerFile string
	CallerLine int
	MethodName string
	Cancel     context.CancelFunc
	Done       chan struct{}
}

type Supervisor struct {
	wg      sync.WaitGroup
	errChan chan error
	ctx     context.Context
	cancel  context.CancelFunc
	logger  *logger.Logger
	doneCh  chan struct{}
	once    sync.Once
	active  sync.Map // map[string]goroutineInfo
}

func NewSupervisor(ctx context.Context, cancel context.CancelFunc, logger *logger.Logger) *Supervisor {
	return &Supervisor{
		errChan: make(chan error, 1),
		ctx:     ctx,
		cancel:  cancel,
		logger:  logger,
		doneCh:  make(chan struct{}),
	}
}

func (s *Supervisor) storeMetadata(name, origin string, cancel context.CancelFunc) (string, chan struct{}) {
	id := uuid.New().String()
	finalName := name + ":" + id
	file, line := callerInfo(3)
	done := make(chan struct{})
	s.active.Store(finalName, goroutineInfo{
		StartTime:  time.Now(),
		Origin:     origin,
		CallerFile: file,
		CallerLine: line,
		MethodName: name,
		Cancel:     cancel,
		Done:       done,
	})
	return finalName, done
}

func callerInfo(skip int) (string, int) {
	if _, file, line, ok := runtime.Caller(skip); ok {
		return file, line
	}
	return "unknown", 0
}

func (s *Supervisor) Go(name string, fn func(ctx context.Context) error) {
	s.wg.Add(1)
	subCtx, subCancel := context.WithCancel(s.ctx)
	finalName, done := s.storeMetadata(name, "framework", subCancel)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				s.logger.Error(subCtx, "🔥 Panic in goroutine", map[string]interface{}{"name": finalName, "recover": r})
			}
			s.active.Delete(finalName)
			close(done)
			s.logger.Info(s.ctx, "✅ Supervisor goroutine finished", map[string]interface{}{"name": finalName})
			s.wg.Done()
		}()

		err := fn(subCtx)
		if err != nil && s.ctx.Err() == nil {
			select {
			case s.errChan <- err:
			default:
			}
			s.logger.Error(subCtx, "💥 Goroutine crashed", map[string]interface{}{"name": finalName, "error": err.Error()})
		}
	}()

	s.logger.Info(subCtx, "🚀 Supervisor started goroutine", map[string]interface{}{"name": finalName})
}

func (s *Supervisor) ErrorChannel() <-chan error {
	return s.errChan
}

func (s *Supervisor) GoLoop(name string, fn func(ctx context.Context) (time.Duration, error)) {
	s.wg.Add(1)
	subCtx, subCancel := context.WithCancel(s.ctx)
	finalName, done := s.storeMetadata(name, "framework", subCancel)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				s.logger.Error(subCtx, "🔥 Panic in loop", map[string]interface{}{"name": finalName, "recover": r})
			}
			s.active.Delete(finalName)
			close(done)
			s.logger.Info(s.ctx, "✅ Supervisor loop finished", map[string]interface{}{"name": finalName})
			s.wg.Done()
		}()

		for {
			select {
			case <-subCtx.Done():
				s.logger.Info(s.ctx, "🛑 Loop exiting (ctx canceled)", map[string]interface{}{"name": finalName, "reason": subCtx.Err().Error()})
				return
			default:
			}

			start := time.Now()
			nextInterval, err := fn(subCtx)
			if err != nil {
				//s.logger.Error("💥 Supervisor loop error, triggering shutdown", map[string]interface{}{"name": finalName, "error": err.Error()})
				select {
				case s.errChan <- err:
				default:
				}
				return
			}

			elapsed := time.Since(start)
			if nextInterval < minLoopDelay {
				nextInterval = minLoopDelay
			}
			sleep := nextInterval - elapsed
			if sleep > 0 {
				select {
				case <-subCtx.Done():
					s.logger.Info(s.ctx, "🛑 Loop interrupted during sleep", map[string]interface{}{"name": finalName})
					return
				case <-time.After(sleep):
				}
			}
		}
	}()

	s.logger.Info(subCtx, "🔁 Supervisor started loop", map[string]interface{}{"name": finalName})
}

func (s *Supervisor) WaitAndShutdown(onShutdown func()) {
	select {
	case <-s.ctx.Done():
		s.logger.Info(s.ctx, "🛑 Shutdown triggered by context cancellation", nil)
		//case err := <-s.errChan:
		//	s.logger.Error("💥 Shutdown due to error", map[string]interface{}{"error": err.Error()})
	}

	s.logger.Info(s.ctx, "📋 Waiting for goroutines to finish...", nil)
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		onShutdown()
		close(done)
	}()

	select {
	case <-done:
		s.logger.Info(s.ctx, "✅ All goroutines shut down cleanly", nil)
	case <-time.After(shutdownTimeout):
		s.logger.Error(s.ctx, "❌ Shutdown timed out. Dumping active goroutines:", nil)
		s.active.Range(func(key, value any) bool {
			info := value.(goroutineInfo)
			s.logger.Warn(s.ctx, "🧵 Possibly stuck", map[string]interface{}{
				"name":        key,
				"start_time":  info.StartTime.String(),
				"caller_file": info.CallerFile,
				"caller_line": info.CallerLine,
				"method":      info.MethodName,
			})
			return true
		})
		time.Sleep(1 * time.Second) // Optional: let logs flush
		os.Exit(1)

	}

	s.once.Do(func() {
		close(s.doneCh)
	})
}

func (s *Supervisor) Shutdown() {
	s.cancel()
}

func (s *Supervisor) Done() <-chan struct{} {
	return s.doneCh
}

func (s *Supervisor) StopAll() {
	s.Shutdown()
	s.wg.Wait()
}

func (s *Supervisor) IsHealthy() bool {
	healthy := true
	now := time.Now()
	activeCount := 0

	s.active.Range(func(key, value any) bool {
		info := value.(goroutineInfo)
		uptime := now.Sub(info.StartTime)
		activeCount++

		entry := map[string]interface{}{
			"name":        key,
			"uptime":      uptime.String(),
			"origin":      info.Origin,
			"caller_file": info.CallerFile,
			"caller_line": info.CallerLine,
			"method":      info.MethodName,
		}

		if uptime > time.Minute {
			s.logger.Warn(s.ctx, "⚠️ Long-running goroutine", entry)
			// healthy = false // Enable if desired
		} else {
			s.logger.Info(s.ctx, "🧵 Active goroutine", entry)
		}
		return true
	})

	s.logger.Info(s.ctx, "📊 Supervisor active count", map[string]interface{}{
		"count": activeCount,
	})

	return healthy
}
