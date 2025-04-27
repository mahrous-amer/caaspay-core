package service

import (
	"time"
)

// ServiceLifecycle handles startup, readiness, liveness probes.
type ServiceLifecycle struct {
	started   bool
	ready     bool
	shutdown  bool
	timestamp time.Time
}

func NewServiceLifecycle() *ServiceLifecycle {
	return &ServiceLifecycle{
		started:   false,
		ready:     false,
		shutdown:  false,
		timestamp: time.Now(),
	}
}

func (l *ServiceLifecycle) MarkStarted() {
	l.started = true
	l.timestamp = time.Now()
}

func (l *ServiceLifecycle) MarkReady() {
	l.ready = true
}

func (l *ServiceLifecycle) MarkShutdown() {
	l.shutdown = true
}

func (l *ServiceLifecycle) IsStarted() bool {
	return l.started
}

func (l *ServiceLifecycle) IsReady() bool {
	return l.ready
}

func (l *ServiceLifecycle) IsShutdown() bool {
	return l.shutdown
}

func (l *ServiceLifecycle) Uptime() time.Duration {
	return time.Since(l.timestamp)
}
