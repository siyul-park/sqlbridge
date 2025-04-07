package schema

import (
	"fmt"
	"sync"
)

type Registry interface {
	Catalog(name string) (Catalog, error)
}

type InMemoryRegistry struct {
	catalogs map[string]Catalog
	mu       sync.RWMutex
}

func NewInMemoryRegistry(catalogs map[string]Catalog) *InMemoryRegistry {
	if catalogs == nil {
		catalogs = make(map[string]Catalog)
	}
	return &InMemoryRegistry{catalogs: catalogs}
}

func (r *InMemoryRegistry) Catalog(name string) (Catalog, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	catalog, ok := r.catalogs[name]
	if !ok {
		return nil, fmt.Errorf("catalog not found: %v", name)
	}
	return catalog, nil
}
