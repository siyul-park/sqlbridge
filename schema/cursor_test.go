package schema

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
)

func TestReadAll(t *testing.T) {
	rows := []Row{
		{
			Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("id")}, {Name: sqlparser.NewColIdent("name")}},
			Values:  []sqltypes.Value{sqltypes.NewInt64(0), sqltypes.MakeTrusted(sqltypes.VarChar, []byte("foo"))},
		},
		{
			Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("id")}, {Name: sqlparser.NewColIdent("name")}},
			Values:  []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.MakeTrusted(sqltypes.VarChar, []byte("foo"))},
		},
	}

	cursor := NewInMemoryCursor(rows)
	defer cursor.Close()

	r, err := ReadAll(cursor)
	require.NoError(t, err)
	require.Equal(t, rows, r)
}

func TestInMemoryCursor_Next(t *testing.T) {
	rows := []Row{
		{
			Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("id")}, {Name: sqlparser.NewColIdent("name")}},
			Values:  []sqltypes.Value{sqltypes.NewInt64(0), sqltypes.MakeTrusted(sqltypes.VarChar, []byte("foo"))},
		},
		{
			Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("id")}, {Name: sqlparser.NewColIdent("name")}},
			Values:  []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.MakeTrusted(sqltypes.VarChar, []byte("foo"))},
		},
	}

	cursor := NewInMemoryCursor(rows)
	defer cursor.Close()

	next, err := cursor.Next()
	require.NoError(t, err)
	require.Equal(t, rows[0], next)

	next, err = cursor.Next()
	require.NoError(t, err)
	require.Equal(t, rows[1], next)

	next, err = cursor.Next()
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, Row{}, next)
}

func TestMappedCursor_Next(t *testing.T) {
	rows := []Row{
		{
			Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("id")}, {Name: sqlparser.NewColIdent("name")}},
			Values:  []sqltypes.Value{sqltypes.NewInt64(0), sqltypes.MakeTrusted(sqltypes.VarChar, []byte("foo"))},
		},
		{
			Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("id")}, {Name: sqlparser.NewColIdent("name")}},
			Values:  []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.MakeTrusted(sqltypes.VarChar, []byte("foo"))},
		},
	}

	cursor1 := NewInMemoryCursor(rows)
	defer cursor1.Close()

	cursor2 := NewMappedCursor(cursor1, func(row Row) (Row, error) {
		return row, nil
	})
	defer cursor2.Close()

	next, err := cursor2.Next()
	require.NoError(t, err)
	require.Equal(t, rows[0], next)

	next, err = cursor2.Next()
	require.NoError(t, err)
	require.Equal(t, rows[1], next)

	next, err = cursor2.Next()
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, Row{}, next)
}
