package schema

import (
	"testing"

	"github.com/go-faker/faker/v4"
	"github.com/stretchr/testify/require"
)

func TestCompositeRegistry_Catalog(t *testing.T) {
	name := faker.UUIDDigit()
	catalog := NewInMemoryCatalog(nil)

	upper := NewInMemoryRegistry(nil)
	lower := NewInMemoryRegistry(map[string]Catalog{
		name: catalog,
	})

	registry := NewCompositeRegistry(upper, lower)

	ctl, err := registry.Catalog(name)
	require.NoError(t, err)
	require.Equal(t, catalog, ctl)
}

func TestInMemoryRegistry_Catalog(t *testing.T) {
	name := faker.UUIDDigit()
	catalog := NewInMemoryCatalog(nil)

	registry := NewInMemoryRegistry(map[string]Catalog{
		name: catalog,
	})

	ctl, err := registry.Catalog(name)
	require.NoError(t, err)
	require.Equal(t, catalog, ctl)
}
