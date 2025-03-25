package schema

import (
	"sync"
)

type Registry struct {
	schemas map[string]Schema
	mu      sync.RWMutex
}

func NewRegistry() *Registry {
	return &Registry{schemas: make(map[string]Schema)}
}

func (r *Registry) AddSchema(name string, schema Schema) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.schemas[name]; exists {
		return false
	}

	r.schemas[name] = schema
	return true
}

func (r *Registry) RemoveSchema(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.schemas[name]; !exists {
		return false
	}

	delete(r.schemas, name)
	return true
}

func (r *Registry) Schema(name string) (Schema, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	sc, ok := r.schemas[name]
	return sc, ok
}
