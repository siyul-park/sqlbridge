package driver

import (
	"github.com/go-faker/faker/v4"
	"github.com/siyul-park/sqlbridge/schema"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestConnection_Prepare(t *testing.T) {
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

	require.NoError(t, stmt.Close())
}
