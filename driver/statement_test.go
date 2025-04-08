package driver

import (
	"database/sql/driver"
	"fmt"
	"testing"

	"github.com/go-faker/faker/v4"
	"github.com/siyul-park/sqlbridge/schema"
	"github.com/stretchr/testify/require"
)

func TestStatement_NumInput(t *testing.T) {
	name := faker.Word()
	table := faker.Word()

	catalog := schema.NewInMemoryCatalog(map[string]schema.Table{
		table: schema.NewInMemoryTable(nil),
	})
	registry := schema.NewInMemoryRegistry(map[string]schema.Catalog{
		name: catalog,
	})

	drv := New(WithRegistry(registry))

	conn, err := drv.Open(name)
	require.NoError(t, err)
	require.NotNil(t, conn)

	stmt, err := conn.Prepare(fmt.Sprintf("SELECT * FROM %s WHERE name = ?", table))
	require.NoError(t, err)
	require.NotNil(t, stmt)

	input := stmt.NumInput()
	require.Equal(t, 1, input)

	require.NoError(t, stmt.Close())
}

func TestStatement_Exec(t *testing.T) {
	name := faker.Word()
	table := faker.Word()

	catalog := schema.NewInMemoryCatalog(map[string]schema.Table{
		table: schema.NewInMemoryTable(nil),
	})
	registry := schema.NewInMemoryRegistry(map[string]schema.Catalog{
		name: catalog,
	})

	drv := New(WithRegistry(registry))

	conn, err := drv.Open(name)
	require.NoError(t, err)
	require.NotNil(t, conn)

	stmt, err := conn.Prepare(fmt.Sprintf("SELECT * FROM %s WHERE name = ?", table))
	require.NoError(t, err)
	require.NotNil(t, stmt)

	result, err := stmt.Exec([]driver.Value{faker.Name()})
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestStatement_Query(t *testing.T) {
	name := faker.Word()
	table := faker.Word()

	catalog := schema.NewInMemoryCatalog(map[string]schema.Table{
		table: schema.NewInMemoryTable(nil),
	})
	registry := schema.NewInMemoryRegistry(map[string]schema.Catalog{
		name: catalog,
	})

	drv := New(WithRegistry(registry))

	conn, err := drv.Open(name)
	require.NoError(t, err)
	require.NotNil(t, conn)

	stmt, err := conn.Prepare(fmt.Sprintf("SELECT * FROM %s WHERE name = ?", table))
	require.NoError(t, err)
	require.NotNil(t, stmt)

	rows, err := stmt.Query([]driver.Value{faker.Name()})
	require.NoError(t, err)
	require.NotNil(t, rows)
}
