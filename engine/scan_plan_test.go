package engine

import (
	"context"
	"testing"
	"time"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/stretchr/testify/require"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/querypb"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
)

func TestScanPlan_Run(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	t1 := schema.NewInMemoryTable([]schema.Row{
		{
			Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("id")}, {Name: sqlparser.NewColIdent("name")}},
			Values:  []sqltypes.Value{sqltypes.NewInt64(0), sqltypes.MakeTrusted(sqltypes.VarChar, []byte("foo"))},
		},
		{
			Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("id")}, {Name: sqlparser.NewColIdent("name")}},
			Values:  []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.MakeTrusted(sqltypes.VarChar, []byte("foo"))},
		},
	})
	t2 := schema.NewInMemoryTable([]schema.Row{
		{
			Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("id")}, {Name: sqlparser.NewColIdent("name")}},
			Values:  []sqltypes.Value{sqltypes.NewInt64(0), sqltypes.MakeTrusted(sqltypes.VarChar, []byte("bar"))},
		},
		{
			Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("id")}, {Name: sqlparser.NewColIdent("name")}},
			Values:  []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.MakeTrusted(sqltypes.VarChar, []byte("bar"))},
		},
	})

	_ = t1.SetIndex(ctx, schema.Index{
		Name:    "id",
		Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("id")}},
	})
	_ = t1.SetIndex(ctx, schema.Index{
		Name:    "name",
		Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("name")}},
	})

	catalog := schema.NewInMemoryCatalog(map[string]schema.Table{
		"t1": t1,
		"t2": t2,
	})

	tests := []struct {
		plan   Plan
		binds  map[string]*querypb.BindVariable
		cursor schema.Cursor
	}{
		{
			plan: &ScanPlan{
				Catalog: catalog,
				Table:   sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")},
			},
			cursor: schema.NewInMemoryCursor([]schema.Row{
				{
					Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("id")}, {Name: sqlparser.NewColIdent("name")}},
					Values:  []sqltypes.Value{sqltypes.NewInt64(0), sqltypes.MakeTrusted(sqltypes.VarChar, []byte("foo"))},
				},
				{
					Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("id")}, {Name: sqlparser.NewColIdent("name")}},
					Values:  []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.MakeTrusted(sqltypes.VarChar, []byte("foo"))},
				},
			}),
		},
		{
			plan: &ScanPlan{
				Catalog: catalog,
				Table:   sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")},
				Expr: &EqualExpr{
					Left:  &IndexExpr{Left: &ColumnExpr{Value: &sqlparser.ColName{Name: sqlparser.NewColIdent("id")}}, Right: &LiteralExpr{Value: sqltypes.NewInt64(0)}},
					Right: &LiteralExpr{Value: sqltypes.NewInt64(0)},
				},
			},
			cursor: schema.NewInMemoryCursor([]schema.Row{
				{
					Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("id")}, {Name: sqlparser.NewColIdent("name")}},
					Values:  []sqltypes.Value{sqltypes.NewInt64(0), sqltypes.MakeTrusted(sqltypes.VarChar, []byte("foo"))},
				},
				{
					Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("id")}, {Name: sqlparser.NewColIdent("name")}},
					Values:  []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.MakeTrusted(sqltypes.VarChar, []byte("foo"))},
				},
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.plan.String(), func(t *testing.T) {
			cursor, err := tt.plan.Run(ctx, tt.binds)
			require.NoError(t, err)

			expected, err := schema.ReadAll(tt.cursor)
			require.NoError(t, err)

			actual, err := schema.ReadAll(cursor)
			require.NoError(t, err)
			require.Equal(t, expected, actual)
		})
	}
}
