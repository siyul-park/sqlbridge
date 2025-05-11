package schema

import (
	"testing"

	"github.com/go-faker/faker/v4"
	"github.com/stretchr/testify/require"
)

func TestCompositeCatalog_Tablet(t *testing.T) {
	name := faker.UUIDDigit()
	table := NewInMemoryTable(nil)

	upper := NewInMemoryCatalog(nil)
	lower := NewInMemoryCatalog(map[string]Table{
		name: table,
	})

	catalog := NewCompositeCatalog(upper, lower)

	tbl, err := catalog.Table(name)
	require.NoError(t, err)
	require.Equal(t, table, tbl)
}

func TestInMemoryCatalog_Table(t *testing.T) {
	name := faker.UUIDDigit()
	table := NewInMemoryTable(nil)

	catalog := NewInMemoryCatalog(map[string]Table{
		name: table,
	})

	tbl, err := catalog.Table(name)
	require.NoError(t, err)
	require.Equal(t, table, tbl)
}
