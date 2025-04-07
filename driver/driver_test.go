package driver

import (
	"testing"

	"github.com/go-faker/faker/v4"
	"github.com/siyul-park/sqlbridge/schema"
	"github.com/stretchr/testify/require"
)

func TestDriver_Open(t *testing.T) {
	name := faker.Word()
	catalog := schema.NewInMemoryCatalog(nil)

	registry := schema.NewInMemoryRegistry(map[string]schema.Catalog{
		name: catalog,
	})

	drv := New(registry)

	conn, err := drv.Open(name)
	require.NoError(t, err)
	require.NotNil(t, conn)
}

func TestDriver_OpenConnector(t *testing.T) {
	name := faker.Word()
	catalog := schema.NewInMemoryCatalog(nil)

	registry := schema.NewInMemoryRegistry(map[string]schema.Catalog{
		name: catalog,
	})

	drv := New(registry)

	connector, err := drv.OpenConnector(name)
	require.NoError(t, err)
	require.NotNil(t, connector)
}
