package driver

import (
	"database/sql/driver"
	"github.com/go-faker/faker/v4"
	"github.com/siyul-park/sqlbridge/schema"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestStatement_NumInput(t *testing.T) {
	drv := New()

	name := faker.Word()
	sc := schema.New(nil)

	ok := drv.AddSchema(name, sc)
	require.True(t, ok)

	conn, err := drv.Open(name)
	require.NoError(t, err)
	require.NotNil(t, conn)

	stmt, err := conn.Prepare("SELECT * FROM users WHERE name = ?")
	require.NoError(t, err)
	require.NotNil(t, stmt)

	input := stmt.NumInput()
	require.Equal(t, 1, input)

	require.NoError(t, stmt.Close())
}

func TestStatement_Exec(t *testing.T) {
	drv := New()

	name := faker.Word()
	sc := schema.New(nil)

	ok := drv.AddSchema(name, sc)
	require.True(t, ok)

	conn, err := drv.Open(name)
	require.NoError(t, err)
	require.NotNil(t, conn)

	stmt, err := conn.Prepare("SELECT * FROM users WHERE name = ?")
	require.NoError(t, err)
	require.NotNil(t, stmt)

	result, err := stmt.Exec([]driver.Value{faker.Name()})
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestStatement_Query(t *testing.T) {
	drv := New()

	name := faker.Word()
	sc := schema.New(nil)

	ok := drv.AddSchema(name, sc)
	require.True(t, ok)

	conn, err := drv.Open(name)
	require.NoError(t, err)
	require.NotNil(t, conn)

	stmt, err := conn.Prepare("SELECT * FROM users WHERE name = ?")
	require.NoError(t, err)
	require.NotNil(t, stmt)

	rows, err := stmt.Query([]driver.Value{faker.Name()})
	require.NoError(t, err)
	require.NotNil(t, rows)
}
