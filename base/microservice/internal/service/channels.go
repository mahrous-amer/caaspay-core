package service

import "sync"

// ChannelsRegistry manages emitter/receiver channels.
type ChannelsRegistry struct {
	mu       sync.RWMutex
	channels map[string]chan []byte
}

func NewChannelsRegistry() *ChannelsRegistry {
	return &ChannelsRegistry{
		channels: make(map[string]chan []byte),
	}
}

func (r *ChannelsRegistry) Register(name string, ch chan []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.channels[name] = ch
}

func (r *ChannelsRegistry) Get(name string) (chan []byte, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ch, ok := r.channels[name]
	return ch, ok
}

func (r *ChannelsRegistry) Delete(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.channels, name)
}
