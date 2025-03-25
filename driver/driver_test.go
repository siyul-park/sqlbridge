package driver

import (
	"testing"

	"github.com/go-faker/faker/v4"
	"github.com/siyul-park/sqlbridge/schema"
	"github.com/stretchr/testify/require"
)

func TestDriver_Open(t *testing.T) {
	drv := New()

	name := faker.Word()
	sc := schema.New(nil)

	ok := drv.AddSchema(name, sc)
	require.True(t, ok)

	conn, err := drv.Open(name)
	require.NoError(t, err)
	require.NotNil(t, conn)
}

func TestDriver_OpenConnector(t *testing.T) {
	drv := New()

	name := faker.Word()
	sc := schema.New(nil)

	ok := drv.AddSchema(name, sc)
	require.True(t, ok)

	connector, err := drv.OpenConnector(name)
	require.NoError(t, err)
	require.NotNil(t, connector)
}
