package engine

import (
	"context"
	"testing"
	"time"

	"github.com/xwb1989/sqlparser"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/stretchr/testify/require"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
)

func TestSubqueryExpr_Eval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	t1 := schema.NewInMemoryTable([]schema.Row{
		{
			Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("id")}, {Name: sqlparser.NewColIdent("name")}},
			Values:  []sqltypes.Value{sqltypes.NewInt64(0), sqltypes.MakeTrusted(sqltypes.VarChar, []byte("foo"))},
		},
		{
			Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("id")}, {Name: sqlparser.NewColIdent("name")}},
			Values:  []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.MakeTrusted(sqltypes.VarChar, []byte("bar"))},
		},
	})

	catalog := schema.NewInMemoryCatalog(map[string]schema.Table{
		"t1": t1,
	})

	tests := []struct {
		input    Plan
		expected Value
	}{
		{
			input: &ProjectionPlan{
				Input: &AliasPlan{
					Input: &ScanPlan{Catalog: catalog, Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
					As:    sqlparser.NewTableIdent("t1"),
				},
				Items: []ProjectionItem{&StartItem{}},
			},
			expected: NewTuple([]Value{
				NewTuple([]Value{NewInt64(0), NewVarChar("foo")}),
				NewTuple([]Value{NewInt64(1), NewVarChar("bar")}),
			}),
		},
		{
			input: &ProjectionPlan{
				Input: &AliasPlan{
					Input: &ScanPlan{Catalog: catalog, Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
					As:    sqlparser.NewTableIdent("t1"),
				},
				Items: []ProjectionItem{&AliasItem{Expr: &IndexExpr{Left: &ColumnExpr{Value: &sqlparser.ColName{Name: sqlparser.NewColIdent("id")}}, Right: &LiteralExpr{Value: sqltypes.NewInt64(0)}}, As: sqlparser.NewColIdent("id")}},
			},
			expected: NewTuple([]Value{
				NewInt64(0),
				NewInt64(1),
			}),
		},
	}

	for _, tt := range tests {
		expr := &SubqueryExpr{Input: tt.input}
		t.Run(expr.String(), func(t *testing.T) {
			actual, err := expr.Eval(ctx, schema.Row{}, nil)
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual)
		})
	}
}
