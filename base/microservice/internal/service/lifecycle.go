package service

import (
	"sync/atomic"
	"time"
)

type ServiceLifecycle struct {
	started   int32
	ready     int32
	shutdown  int32
	timestamp time.Time
}

func NewServiceLifecycle() *ServiceLifecycle {
	return &ServiceLifecycle{
		timestamp: time.Now(),
	}
}

func (l *ServiceLifecycle) MarkStarted() {
	atomic.StoreInt32(&l.started, 1)
	l.timestamp = time.Now()
}

func (l *ServiceLifecycle) MarkReady() {
	atomic.StoreInt32(&l.ready, 1)
}

func (l *ServiceLifecycle) MarkShutdown() {
	atomic.StoreInt32(&l.shutdown, 1)
}

func (l *ServiceLifecycle) IsStarted() bool {
	return atomic.LoadInt32(&l.started) == 1
}

func (l *ServiceLifecycle) IsReady() bool {
	return atomic.LoadInt32(&l.ready) == 1
}

func (l *ServiceLifecycle) IsShutdown() bool {
	return atomic.LoadInt32(&l.shutdown) == 1
}

func (l *ServiceLifecycle) Uptime() time.Duration {
	return time.Since(l.timestamp)
}
