package driver

import (
	"context"
	"testing"
	"time"

	"github.com/go-faker/faker/v4"
	"github.com/siyul-park/sqlbridge/schema"
	"github.com/stretchr/testify/require"
)

func TestConnector_Connect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	name := faker.Word()
	catalog := schema.NewInMemoryCatalog(nil)
	registry := schema.NewRegistry()

	err := registry.SetCatalog(name, catalog)
	require.NoError(t, err)

	drv := New(registry)

	connector, err := drv.OpenConnector(name)
	require.NoError(t, err)
	require.NotNil(t, connector)

	conn, err := connector.Connect(ctx)
	require.NoError(t, err)
	require.NotNil(t, conn)
}

func TestConnector_Driver(t *testing.T) {
	name := faker.Word()
	catalog := schema.NewInMemoryCatalog(nil)
	registry := schema.NewRegistry()

	err := registry.SetCatalog(name, catalog)
	require.NoError(t, err)

	drv := New(registry)

	connector, err := drv.OpenConnector(name)
	require.NoError(t, err)
	require.NotNil(t, connector)

	require.Equal(t, drv, connector.Driver())
}
