package storage

import (
	"context"
	"errors"
	"fmt"

	"github.com/caaspay/caaspay-core/internal/config"
)

// Store defines the interface for storage backends.
type Store interface {
	// Get retrieves a value by key.
	Get(ctx context.Context, key string) (interface{}, error)
	// Set stores a value by key.
	Set(ctx context.Context, key string, value interface{}) error
	// Delete removes a value by key.
	Delete(ctx context.Context, key string) error
}

// NewStore initializes a storage backend based on the configuration.
func NewStore(cfg config.StorageConfig) (Store, error) {
	switch cfg.Type {
	case "inmemory":
		return NewInMemoryStore(), nil
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

// InMemoryStore is a simple in-memory implementation of the Store interface.
type InMemoryStore struct {
	data map[string]interface{}
}

// NewInMemoryStore creates a new instance of InMemoryStore.
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		data: make(map[string]interface{}),
	}
}

// Get retrieves a value by key from the in-memory store.
func (s *InMemoryStore) Get(ctx context.Context, key string) (interface{}, error) {
	if value, exists := s.data[key]; exists {
		return value, nil
	}
	return nil, errors.New("key not found")
}

// Set stores a value by key in the in-memory store.
func (s *InMemoryStore) Set(ctx context.Context, key string, value interface{}) error {
	s.data[key] = value
	return nil
}

// Delete removes a value by key from the in-memory store.
func (s *InMemoryStore) Delete(ctx context.Context, key string) error {
	if _, exists := s.data[key]; exists {
		delete(s.data, key)
		return nil
	}
	return errors.New("key not found")
}
