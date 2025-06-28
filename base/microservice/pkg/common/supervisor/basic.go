package supervisor

import (
	"context"
	"sync"
	"time"
)

type BasicSupervisor struct {
	wg     sync.WaitGroup
	errCh  chan error
	doneCh chan struct{}
}

func NewBasicSupervisor() *BasicSupervisor {
	return &BasicSupervisor{
		errCh:  make(chan error, 1),
		doneCh: make(chan struct{}),
	}
}

func (s *BasicSupervisor) Go(_ string, fn func(context.Context) error) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := fn(context.Background()); err != nil {
			select {
			case s.errCh <- err:
			default:
			}
		}
	}()
}

func (s *BasicSupervisor) GoLoop(_ string, fn func(context.Context) (time.Duration, error)) {
	s.Go("loop", func(ctx context.Context) error {
		for {
			delay, err := fn(ctx)
			if err != nil {
				return err
			}
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})
}

func (s *BasicSupervisor) Shutdown() {
	s.wg.Wait()
	close(s.doneCh)
}

func (s *BasicSupervisor) Done() <-chan struct{} {
	return s.doneCh
}

func (s *BasicSupervisor) ErrorChannel() <-chan error {
	return s.errCh
}
