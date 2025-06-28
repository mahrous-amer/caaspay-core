package storage

import (
	"context"
	"sync"
)

// Store defines the interface for storage backends.
type Store interface {
	Get(key string) (interface{}, error)
	Set(key string, value interface{}) error
	Delete(key string) error
}

// InMemoryStore is a thread-safe in-memory implementation of the Store interface.
type InMemoryStore struct {
	ctx  context.Context
	mu   sync.RWMutex
	data map[string]interface{}
}

// --- Storage abstraction ---
type StorageInterface interface {
	Get(key string) (interface{}, error)
	Set(key string, value interface{}) error
}

// StorageConfig defines storage-related configurations.
type StorageConfig struct {
	Type string `yaml:"type"` // Example: "inmemory", "redis", "sql"
}
