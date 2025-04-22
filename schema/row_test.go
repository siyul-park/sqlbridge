package schema

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
)

func TestRow_Get(t *testing.T) {
	row := Row{
		Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("id")}, {Name: sqlparser.NewColIdent("name")}},
		Values:  []sqltypes.Value{sqltypes.NewInt64(0), sqltypes.MakeTrusted(sqltypes.VarChar, []byte("foo"))},
	}

	val, ok := row.Get(&sqlparser.ColName{Name: sqlparser.NewColIdent("id")})
	require.True(t, ok)
	require.Equal(t, sqltypes.NewInt64(0), val)
}

func TestRow_IsEmpty(t *testing.T) {
	row1 := Row{
		Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("id")}, {Name: sqlparser.NewColIdent("name")}},
		Values:  []sqltypes.Value{sqltypes.NewInt64(0), sqltypes.MakeTrusted(sqltypes.VarChar, []byte("foo"))},
	}
	row2 := Row{}
	require.False(t, row1.IsEmpty())
	require.True(t, row2.IsEmpty())
}
