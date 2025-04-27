package service

import "sync"

// ServiceRegistry holds all active services.
type ServiceRegistry struct {
	mu       sync.RWMutex
	services map[string]interface{}
}

func NewServiceRegistry() *ServiceRegistry {
	return &ServiceRegistry{
		services: make(map[string]interface{}),
	}
}

func (r *ServiceRegistry) Register(name string, svc interface{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.services[name] = svc
}

func (r *ServiceRegistry) Get(name string) (interface{}, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	svc, ok := r.services[name]
	return svc, ok
}

func (r *ServiceRegistry) List() map[string]interface{} {
	r.mu.RLock()
	defer r.mu.RUnlock()
	copied := make(map[string]interface{})
	for k, v := range r.services {
		copied[k] = v
	}
	return copied
}
