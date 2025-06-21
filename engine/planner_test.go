package engine

import (
	"testing"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/stretchr/testify/require"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/querypb"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
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
	dispatcher := NewDispatcher()
	planner := NewPlanner(catalog, dispatcher)

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
			plan: &ProjectionPlan{
				Input: &AliasPlan{
					Input: &ScanPlan{Catalog: catalog, Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
					As:    sqlparser.NewTableIdent("t1"),
				},
				Items: []ProjectionItem{&StartItem{}},
			},
		},
		{
			node: &sqlparser.Select{
				SelectExprs: sqlparser.SelectExprs{&sqlparser.AliasedExpr{Expr: &sqlparser.ColName{Name: sqlparser.NewColIdent("id")}, As: sqlparser.NewColIdent("id")}},
				From: sqlparser.TableExprs{
					&sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
				},
			},
			plan: &ProjectionPlan{
				Input: &AliasPlan{
					Input: &ScanPlan{Catalog: catalog, Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
					As:    sqlparser.NewTableIdent("t1"),
				},
				Items: []ProjectionItem{&AliasItem{Expr: &IndexExpr{Left: &ColumnExpr{Value: &sqlparser.ColName{Name: sqlparser.NewColIdent("id")}}, Right: &LiteralExpr{Value: sqltypes.NewInt64(0)}}, As: sqlparser.NewColIdent("id")}},
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
			plan: &ProjectionPlan{
				Input: &FilterPlan{
					Input: &AliasPlan{
						Input: &ScanPlan{
							Catalog: catalog,
							Table:   sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")},
							Expr: &EqualExpr{
								Left:  &IndexExpr{Left: &ColumnExpr{Value: &sqlparser.ColName{Name: sqlparser.NewColIdent("id")}}, Right: &LiteralExpr{Value: sqltypes.NewInt64(0)}},
								Right: &LiteralExpr{Value: sqltypes.NewInt64(0)},
							},
						},
						As: sqlparser.NewTableIdent("t1"),
					},
					Expr: &EqualExpr{
						Left:  &IndexExpr{Left: &ColumnExpr{Value: &sqlparser.ColName{Name: sqlparser.NewColIdent("id")}}, Right: &LiteralExpr{Value: sqltypes.NewInt64(0)}},
						Right: &LiteralExpr{Value: sqltypes.NewInt64(0)},
					},
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
				GroupBy: sqlparser.GroupBy{
					&sqlparser.ColName{Name: sqlparser.NewColIdent("name")},
				},
			},
			plan: &ProjectionPlan{
				Input: &GroupPlan{
					Input: &AliasPlan{
						Input: &ScanPlan{Catalog: catalog, Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
						As:    sqlparser.NewTableIdent("t1"),
					},
					Exprs: []Expr{
						&IndexExpr{Left: &ColumnExpr{Value: &sqlparser.ColName{Name: sqlparser.NewColIdent("name")}}, Right: &LiteralExpr{Value: sqltypes.NewInt64(0)}},
					},
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
				GroupBy: sqlparser.GroupBy{
					&sqlparser.ColName{Name: sqlparser.NewColIdent("name")},
				},
				Having: &sqlparser.Where{
					Type: sqlparser.HavingStr,
					Expr: &sqlparser.ComparisonExpr{
						Operator: sqlparser.EqualStr,
						Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("name")},
						Right:    &sqlparser.SQLVal{Type: sqlparser.StrVal, Val: []byte("foo")},
					},
				},
			},
			plan: &ProjectionPlan{
				Input: &FilterPlan{
					Input: &GroupPlan{
						Input: &AliasPlan{
							Input: &ScanPlan{Catalog: catalog, Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
							As:    sqlparser.NewTableIdent("t1"),
						},
						Exprs: []Expr{
							&IndexExpr{Left: &ColumnExpr{Value: &sqlparser.ColName{Name: sqlparser.NewColIdent("name")}}, Right: &LiteralExpr{Value: sqltypes.NewInt64(0)}},
						},
					},
					Expr: &EqualExpr{
						Left:  &IndexExpr{Left: &ColumnExpr{Value: &sqlparser.ColName{Name: sqlparser.NewColIdent("name")}}, Right: &LiteralExpr{Value: sqltypes.NewInt64(0)}},
						Right: &LiteralExpr{Value: sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("foo"))},
					},
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
				Distinct: sqlparser.DistinctStr,
			},
			plan: &DistinctPlan{
				Input: &ProjectionPlan{
					Input: &AliasPlan{
						Input: &ScanPlan{Catalog: catalog, Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
						As:    sqlparser.NewTableIdent("t1"),
					},
					Items: []ProjectionItem{&StartItem{}},
				},
			},
		},
		{
			node: &sqlparser.Select{
				SelectExprs: sqlparser.SelectExprs{&sqlparser.StarExpr{}},
				From: sqlparser.TableExprs{
					&sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
				},
				OrderBy: sqlparser.OrderBy{
					&sqlparser.Order{
						Expr:      &sqlparser.ColName{Name: sqlparser.NewColIdent("id")},
						Direction: sqlparser.AscScr,
					},
				},
			},
			plan: &OrderPlan{
				Input: &ProjectionPlan{
					Input: &AliasPlan{
						Input: &ScanPlan{Catalog: catalog, Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
						As:    sqlparser.NewTableIdent("t1"),
					},
					Items: []ProjectionItem{&StartItem{}},
				},
				Expr:      &IndexExpr{Left: &ColumnExpr{Value: &sqlparser.ColName{Name: sqlparser.NewColIdent("id")}}, Right: &LiteralExpr{Value: sqltypes.NewInt64(0)}},
				Direction: sqlparser.AscScr,
			},
		},
		{
			node: &sqlparser.Select{
				SelectExprs: sqlparser.SelectExprs{&sqlparser.StarExpr{}},
				From: sqlparser.TableExprs{
					&sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
				},
				Limit: &sqlparser.Limit{
					Offset:   sqlparser.NewIntVal([]byte("1")),
					Rowcount: sqlparser.NewIntVal([]byte("1")),
				},
			},
			plan: &LimitPlan{
				Input: &ProjectionPlan{
					Input: &AliasPlan{
						Input: &ScanPlan{Catalog: catalog, Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
						As:    sqlparser.NewTableIdent("t1"),
					},
					Items: []ProjectionItem{&StartItem{}},
				},
				Offset: &LiteralExpr{Value: sqltypes.NewInt64(1)},
				Count:  &LiteralExpr{Value: sqltypes.NewInt64(1)},
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
