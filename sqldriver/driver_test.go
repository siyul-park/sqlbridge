package sqldriver

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"

	"github.com/siyul-park/sqlbridge/catalog"
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

func openDB(t *testing.T) *sql.DB {
	t.Helper()
	tbl := catalog.NewInMemoryTable([]catalog.Row{intRow(1, 10), intRow(2, 20), intRow(3, 30)})
	cat := catalog.NewInMemoryCatalog(map[string]catalog.Table{"t": tbl})
	reg := catalog.NewInMemoryRegistry(map[string]catalog.Catalog{"main": cat})

	connector, err := New(WithRegistry(reg)).OpenConnector("main")
	require.NoError(t, err)
	return sql.OpenDB(connector)
}

func TestDriver_Query(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	t.Run("projection with filter through database/sql", func(t *testing.T) {
		rows, err := db.Query("select a, b from t where a > 1")
		require.NoError(t, err)
		defer rows.Close()

		cols, err := rows.Columns()
		require.NoError(t, err)
		assert.Equal(t, []string{"a", "b"}, cols)

		var got [][2]int64
		for rows.Next() {
			var a, b int64
			require.NoError(t, rows.Scan(&a, &b))
			got = append(got, [2]int64{a, b})
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, [][2]int64{{2, 20}, {3, 30}}, got)
	})

	t.Run("select star", func(t *testing.T) {
		rows, err := db.Query("select * from t")
		require.NoError(t, err)
		defer rows.Close()

		var count int
		for rows.Next() {
			count++
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, 3, count)
	})

}

func TestDriver_Exec(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	t.Run("insert then query", func(t *testing.T) {
		res, err := db.Exec("insert into t (a, b) values (4, 40)")
		require.NoError(t, err)
		n, err := res.RowsAffected()
		require.NoError(t, err)
		assert.Equal(t, int64(1), n)

		var count int
		require.NoError(t, db.QueryRow("select count(a) from t").Scan(&count))
		assert.Equal(t, 4, count)
	})

	t.Run("update matching rows", func(t *testing.T) {
		res, err := db.Exec("update t set b = 99 where a = 1")
		require.NoError(t, err)
		n, err := res.RowsAffected()
		require.NoError(t, err)
		assert.Equal(t, int64(1), n)

		var b int
		require.NoError(t, db.QueryRow("select b from t where a = 1").Scan(&b))
		assert.Equal(t, 99, b)
	})

	t.Run("delete matching rows", func(t *testing.T) {
		_, err := db.Exec("delete from t where a = 2")
		require.NoError(t, err)

		var count int
		require.NoError(t, db.QueryRow("select count(a) from t where a = 2").Scan(&count))
		assert.Equal(t, 0, count)
	})
}
