package plan

import (
	"testing"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"

	"github.com/stretchr/testify/require"
	"github.com/xwb1989/sqlparser"
)

func TestPlanner_Plan(t *testing.T) {
	t1 := schema.NewInMemoryTable([]schema.Row{
		{Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("id")}}, Values: []sqltypes.Value{sqltypes.NewInt64(0)}},
	})
	t2 := schema.NewInMemoryTable([]schema.Row{
		{Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("id")}}, Values: []sqltypes.Value{sqltypes.NewInt64(0)}},
	})

	catalog := schema.NewInMemoryCatalog(map[string]schema.Table{
		"t1": t1,
		"t2": t2,
	})
	planner := NewPlanner(catalog)

	tests := []struct {
		node sqlparser.Statement
		plan Plan
	}{
		{
			node: &sqlparser.Select{
				SelectExprs: sqlparser.SelectExprs{&sqlparser.StarExpr{}},
				From: sqlparser.TableExprs{
					&sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
				},
			},
			plan: &Projection{
				Input: &Alias{
					Input: &Scan{Catalog: catalog, Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}, As: sqlparser.NewTableIdent("t1"),
				},
				Items: []ProjectionItem{&StartItem{}},
			},
		},
		{
			node: &sqlparser.Select{
				SelectExprs: sqlparser.SelectExprs{&sqlparser.StarExpr{}},
				From: sqlparser.TableExprs{
					&sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
				},
				Where: &sqlparser.Where{
					Type: sqlparser.WhereStr,
					Expr: &sqlparser.ComparisonExpr{
						Operator: sqlparser.EqualStr,
						Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("id")},
						Right:    &sqlparser.SQLVal{Type: sqlparser.IntVal, Val: []byte("0")},
					},
				},
			},
			plan: &Projection{
				Input: &Filter{
					Input: &Alias{
						Input: &Scan{Catalog: catalog, Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}, As: sqlparser.NewTableIdent("t1"),
					},
					Expr: &Equal{
						Left:  &Column{Value: &sqlparser.ColName{Name: sqlparser.NewColIdent("id")}},
						Right: &Literal{Value: &querypb.BindVariable{Type: querypb.Type_INT64, Value: []byte("0")}},
					},
				},
				Items: []ProjectionItem{&StartItem{}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(sqlparser.String(tt.node), func(t *testing.T) {
			plan, err := planner.Plan(tt.node)
			require.NoError(t, err)
			require.Equal(t, tt.plan, plan)
		})
	}
}
