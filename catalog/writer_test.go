package catalog

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
)

func intRow(col string, n int64) Row {
	return Row{
		Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent(col)}},
		Values:  []sqltypes.Value{sqltypes.NewInt64(n)},
	}
}

func TestInMemoryTable_Insert(t *testing.T) {
	tbl := NewInMemoryTable(nil)
	n, err := tbl.Insert(context.Background(), intRow("a", 1), intRow("a", 2))
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)

	rows, err := ReadAll(must(tbl.Scan(context.Background())))
	require.NoError(t, err)
	assert.Len(t, rows, 2)
}

func TestInMemoryTable_Delete(t *testing.T) {
	tbl := NewInMemoryTable([]Row{intRow("a", 1), intRow("a", 2), intRow("a", 3)})

	n, err := tbl.Delete(context.Background(), intRow("a", 2))
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)

	rows, err := ReadAll(must(tbl.Scan(context.Background())))
	require.NoError(t, err)
	assert.Len(t, rows, 2)

	n, err = tbl.Delete(context.Background(), intRow("a", 99))
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
