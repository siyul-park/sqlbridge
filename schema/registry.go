package schema

import (
	"sync"

	"github.com/pkg/errors"
)

type Registry interface {
	Catalog(name string) (Catalog, error)
}

type CompositeRegistry struct {
	registries []Registry
}

type InMemoryRegistry struct {
	catalogs map[string]Catalog
	mu       sync.RWMutex
}

var ErrCatalogNotFound = errors.New("catalog not found")

var (
	_ Registry = (*CompositeRegistry)(nil)
	_ Registry = (*InMemoryRegistry)(nil)
)

func NewCompositeRegistry(registries ...Registry) *CompositeRegistry {
	return &CompositeRegistry{registries: registries}
}

func NewInMemoryRegistry(catalogs map[string]Catalog) *InMemoryRegistry {
	if catalogs == nil {
		catalogs = make(map[string]Catalog)
	}
	return &InMemoryRegistry{catalogs: catalogs}
}

func (r *CompositeRegistry) Catalog(name string) (Catalog, error) {
	for _, registry := range r.registries {
		catalog, err := registry.Catalog(name)
		if err == nil {
			return catalog, nil
		}
		if !errors.Is(err, ErrCatalogNotFound) {
			return nil, err
		}
	}
	return nil, errors.WithStack(ErrCatalogNotFound)
}

func (r *InMemoryRegistry) Catalog(name string) (Catalog, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	catalog, ok := r.catalogs[name]
	if !ok {
		return nil, errors.WithStack(ErrCatalogNotFound)
	}
	return catalog, nil
}
