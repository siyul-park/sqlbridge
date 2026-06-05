package compile

import (
	"context"
	"testing"

	"github.com/siyul-park/minivm/interp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"

	"github.com/siyul-park/sqlbridge/catalog"
	"github.com/siyul-park/sqlbridge/runtime"
)

func col(name string) *sqlparser.ColName {
	return &sqlparser.ColName{Name: sqlparser.NewColIdent(name)}
}

func intRow(a, b int64) catalog.Row {
	return catalog.Row{
		Columns: []*sqlparser.ColName{col("a"), col("b")},
		Values:  []sqltypes.Value{sqltypes.NewInt64(a), sqltypes.NewInt64(b)},
	}
}

func fixture() catalog.Catalog {
	tbl := catalog.NewInMemoryTable([]catalog.Row{intRow(1, 10), intRow(2, 20), intRow(3, 30)})
	return catalog.NewInMemoryCatalog(map[string]catalog.Table{"t": tbl})
}

func run(t *testing.T, prog *Program) []catalog.Row {
	t.Helper()
	vm := interp.New(prog.Program)
	defer vm.Close()

	res := runtime.NewResult(prog.Columns)
	sess := runtime.NewSession(res)
	ctx := runtime.WithSession(context.Background(), sess)
	require.NoError(t, vm.Run(ctx))
	return res.Rows()
}

func compileSQL(t *testing.T, cat catalog.Catalog, query string) *Program {
	t.Helper()
	stmt, err := sqlparser.Parse(query)
	require.NoError(t, err)
	prog, err := New(cat).Compile(stmt)
	require.NoError(t, err)
	return prog
}

func TestCompiler_Compile(t *testing.T) {
	cat := fixture()

	t.Run("projection with WHERE filter", func(t *testing.T) {
		prog := compileSQL(t, cat, "select a, b from t where a > 1")
		rows := run(t, prog)

		require.Len(t, rows, 2)
		assert.Equal(t, "2", rows[0].Values[0].ToString())
		assert.Equal(t, "20", rows[0].Values[1].ToString())
		assert.Equal(t, "3", rows[1].Values[0].ToString())
	})

	t.Run("SELECT * emits full rows", func(t *testing.T) {
		prog := compileSQL(t, cat, "select * from t")
		rows := run(t, prog)
		require.Len(t, rows, 3)
		assert.Equal(t, "1", rows[0].Values[0].ToString())
	})

	t.Run("arithmetic projection", func(t *testing.T) {
		prog := compileSQL(t, cat, "select a + b from t where a > 1")
		rows := run(t, prog)
		require.Len(t, rows, 2)
		assert.Equal(t, "22", rows[0].Values[0].ToString())
		assert.Equal(t, "33", rows[1].Values[0].ToString())
	})

	t.Run("compound predicate", func(t *testing.T) {
		prog := compileSQL(t, cat, "select a from t where a >= 2 and b < 30")
		rows := run(t, prog)
		require.Len(t, rows, 1)
		assert.Equal(t, "2", rows[0].Values[0].ToString())
	})

	t.Run("limit and offset", func(t *testing.T) {
		prog := compileSQL(t, cat, "select a from t limit 1 offset 1")
		rows := run(t, prog)
		require.Len(t, rows, 1)
		assert.Equal(t, "2", rows[0].Values[0].ToString())
	})

	t.Run("unsupported statement is rejected", func(t *testing.T) {
		stmt, err := sqlparser.Parse("insert into t (a) values (1)")
		require.NoError(t, err)
		_, err = New(cat).Compile(stmt)
		assert.ErrorIs(t, err, ErrUnsupported)
	})
}
