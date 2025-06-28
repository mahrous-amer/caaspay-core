package storage

import (
	"context"
	"errors"
	"fmt"
	//	"sync"
)

// NewStore initializes a storage backend based on the configuration.
func NewStore(cfg StorageConfig) (Store, error) {
	switch cfg.Type {
	case "inmemory":
		return NewInMemoryStore(context.Background()), nil
	case "redis":
		// Placeholder for Redis storage implementation
		return nil, errors.New("Redis storage not implemented")
	case "sql":
		// Placeholder for SQL storage implementation
		return nil, errors.New("SQL storage not implemented")
	default:
		return nil, fmt.Errorf("unsupported storage type: %s", cfg.Type)
	}
}

// NewInMemoryStore creates a new instance of InMemoryStore.
func NewInMemoryStore(ctx context.Context) *InMemoryStore {
	return &InMemoryStore{
		ctx:  ctx,
		data: make(map[string]interface{}),
	}
}

// Get retrieves a value by key from the in-memory store.
func (s *InMemoryStore) Get(key string) (interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if value, exists := s.data[key]; exists {
		return value, nil
	}
	return nil, errors.New("key not found")
}

// Set stores a value by key in the in-memory store.
func (s *InMemoryStore) Set(key string, value interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = value
	return nil
}

// Delete removes a value by key from the in-memory store.
func (s *InMemoryStore) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.data[key]; exists {
		delete(s.data, key)
		return nil
	}
	return errors.New("key not found")
}
