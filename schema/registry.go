package schema

import (
	"fmt"
	"sync"
)

type Registry struct {
	catalogs map[string]Catalog
	mu       sync.RWMutex
}

func NewRegistry() *Registry {
	return &Registry{catalogs: make(map[string]Catalog)}
}

func (r *Registry) SetCatalog(name string, catalog Catalog) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	_, ok := r.catalogs[name]
	if ok {
		return fmt.Errorf("catalog already exists: %v", name)
	}

	r.catalogs[name] = catalog
	return nil
}

func (r *Registry) Catalog(name string) (Catalog, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	catalog, ok := r.catalogs[name]
	if !ok {
		return nil, fmt.Errorf("catalog not found: %v", name)
	}
	return catalog, nil
}
