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

	drv := New()

	name := faker.Word()
	sc := schema.New(nil)

	ok := drv.AddSchema(name, sc)
	require.True(t, ok)

	connector, err := drv.OpenConnector(name)
	require.NoError(t, err)
	require.NotNil(t, connector)

	conn, err := connector.Connect(ctx)
	require.NoError(t, err)
	require.NotNil(t, conn)
}

func TestConnector_Driver(t *testing.T) {
	drv := New()

	name := faker.Word()
	sc := schema.New(nil)

	ok := drv.AddSchema(name, sc)
	require.True(t, ok)

	connector, err := drv.OpenConnector(name)
	require.NoError(t, err)
	require.NotNil(t, connector)

	require.Equal(t, drv, connector.Driver())
}
