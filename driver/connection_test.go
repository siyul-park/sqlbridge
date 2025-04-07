package driver

import (
	"testing"

	"github.com/go-faker/faker/v4"
	"github.com/siyul-park/sqlbridge/schema"
	"github.com/stretchr/testify/require"
)

func TestConnection_Prepare(t *testing.T) {
	name := faker.Word()
	catalog := schema.NewInMemoryCatalog(nil)

	registry := schema.NewInMemoryRegistry(map[string]schema.Catalog{
		name: catalog,
	})

	drv := New(registry)

	conn, err := drv.Open(name)
	require.NoError(t, err)
	require.NotNil(t, conn)

	stmt, err := conn.Prepare("SELECT * FROM users WHERE name = ?")
	require.NoError(t, err)
	require.NotNil(t, stmt)

	require.NoError(t, stmt.Close())
}
