package plan

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xwb1989/sqlparser"
)

func TestPlanner_Plan(t *testing.T) {
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
			plan: &ProjectPlan{
				Input: &AliasPlan{Input: &ScanPlan{Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}, Alias: sqlparser.NewTableIdent("t1")},
				Exprs: sqlparser.SelectExprs{&sqlparser.StarExpr{}},
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
						Operator: sqlparser.GreaterThanStr,
						Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("age")},
						Right:    sqlparser.NewIntVal([]byte("18")),
					},
				},
			},
			plan: &ProjectPlan{
				Input: &FilterPlan{
					Input: &AliasPlan{Input: &ScanPlan{Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}, Alias: sqlparser.NewTableIdent("t1")},
					Expr: &sqlparser.ComparisonExpr{
						Operator: sqlparser.GreaterThanStr,
						Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("age")},
						Right:    sqlparser.NewIntVal([]byte("18")),
					},
				},
				Exprs: sqlparser.SelectExprs{&sqlparser.StarExpr{}},
			},
		},
		{
			node: &sqlparser.Select{
				SelectExprs: sqlparser.SelectExprs{
					&sqlparser.AliasedExpr{Expr: &sqlparser.FuncExpr{Name: sqlparser.NewColIdent("COUNT"), Exprs: sqlparser.SelectExprs{&sqlparser.StarExpr{}}}},
					&sqlparser.AliasedExpr{Expr: &sqlparser.ColName{Name: sqlparser.NewColIdent("name")}},
				},
				From: sqlparser.TableExprs{
					&sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
				},
				GroupBy: sqlparser.GroupBy{
					&sqlparser.ColName{Name: sqlparser.NewColIdent("name")},
				},
				Having: &sqlparser.Where{
					Type: sqlparser.HavingStr,
					Expr: &sqlparser.ComparisonExpr{
						Left:     &sqlparser.FuncExpr{Name: sqlparser.NewColIdent("COUNT"), Exprs: sqlparser.SelectExprs{&sqlparser.StarExpr{}}},
						Operator: sqlparser.GreaterEqualStr,
						Right:    sqlparser.NewIntVal([]byte("10")),
					},
				},
			},
			plan: &ProjectPlan{
				Input: &FilterPlan{
					Input: &GroupPlan{
						Input: &AliasPlan{Input: &ScanPlan{Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}, Alias: sqlparser.NewTableIdent("t1")},
						Exprs: sqlparser.GroupBy{
							&sqlparser.ColName{Name: sqlparser.NewColIdent("name")},
						},
					},
					Expr: &sqlparser.ComparisonExpr{
						Left:     &sqlparser.FuncExpr{Name: sqlparser.NewColIdent("COUNT"), Exprs: sqlparser.SelectExprs{&sqlparser.StarExpr{}}},
						Operator: sqlparser.GreaterEqualStr,
						Right:    sqlparser.NewIntVal([]byte("10")),
					},
				},
				Exprs: sqlparser.SelectExprs{
					&sqlparser.AliasedExpr{Expr: &sqlparser.FuncExpr{Name: sqlparser.NewColIdent("COUNT"), Exprs: sqlparser.SelectExprs{&sqlparser.StarExpr{}}}},
					&sqlparser.AliasedExpr{Expr: &sqlparser.ColName{Name: sqlparser.NewColIdent("name")}},
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
						Direction: sqlparser.DescScr,
					},
				},
			},
			plan: &OrderPlan{
				Input: &ProjectPlan{
					Input: &AliasPlan{Input: &ScanPlan{Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}, Alias: sqlparser.NewTableIdent("t1")},
					Exprs: sqlparser.SelectExprs{&sqlparser.StarExpr{}},
				},
				Exprs: sqlparser.OrderBy{
					&sqlparser.Order{Expr: &sqlparser.ColName{Name: sqlparser.NewColIdent("id")}, Direction: sqlparser.DescScr},
				},
			},
		},
		{
			node: &sqlparser.Select{
				SelectExprs: sqlparser.SelectExprs{&sqlparser.StarExpr{}},
				From: sqlparser.TableExprs{
					&sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
				},
				Limit: &sqlparser.Limit{
					Rowcount: sqlparser.NewIntVal([]byte("10")),
				},
			},
			plan: &LimitPlan{
				Input: &ProjectPlan{
					Input: &AliasPlan{Input: &ScanPlan{Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}, Alias: sqlparser.NewTableIdent("t1")},
					Exprs: sqlparser.SelectExprs{&sqlparser.StarExpr{}},
				},
				Exprs: &sqlparser.Limit{
					Rowcount: sqlparser.NewIntVal([]byte("10")),
				},
			},
		},
		{
			node: &sqlparser.Select{
				SelectExprs: sqlparser.SelectExprs{&sqlparser.StarExpr{}},
				From: sqlparser.TableExprs{
					&sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
					&sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: sqlparser.NewTableIdent("t2")}},
				},
			},
			plan: &ProjectPlan{
				Input: &JoinPlan{
					Left:  &AliasPlan{Input: &ScanPlan{Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}, Alias: sqlparser.NewTableIdent("t1")},
					Right: &AliasPlan{Input: &ScanPlan{Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t2")}}, Alias: sqlparser.NewTableIdent("t2")},
					Join:  sqlparser.JoinStr,
				},
				Exprs: sqlparser.SelectExprs{&sqlparser.StarExpr{}},
			},
		},
		{
			node: &sqlparser.Select{
				SelectExprs: sqlparser.SelectExprs{&sqlparser.StarExpr{}},
				From: sqlparser.TableExprs{
					&sqlparser.ParenTableExpr{
						Exprs: sqlparser.TableExprs{
							&sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
							&sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: sqlparser.NewTableIdent("t2")}},
						},
					},
				},
			},
			plan: &ProjectPlan{
				Input: &JoinPlan{
					Left:  &AliasPlan{Input: &ScanPlan{Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}, Alias: sqlparser.NewTableIdent("t1")},
					Right: &AliasPlan{Input: &ScanPlan{Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t2")}}, Alias: sqlparser.NewTableIdent("t2")},
					Join:  sqlparser.JoinStr,
				},
				Exprs: sqlparser.SelectExprs{&sqlparser.StarExpr{}},
			},
		},
		{
			node: &sqlparser.Select{
				SelectExprs: sqlparser.SelectExprs{&sqlparser.StarExpr{}},
				From: sqlparser.TableExprs{
					&sqlparser.JoinTableExpr{
						LeftExpr:  &sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
						RightExpr: &sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: sqlparser.NewTableIdent("t2")}},
						Join:      sqlparser.JoinStr,
						Condition: sqlparser.JoinCondition{
							On: &sqlparser.ComparisonExpr{
								Operator: sqlparser.EqualStr,
								Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("id"), Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
								Right:    &sqlparser.ColName{Name: sqlparser.NewColIdent("id"), Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("t2")}},
							},
						},
					},
				},
			},
			plan: &ProjectPlan{
				Input: &JoinPlan{
					Left:  &AliasPlan{Input: &ScanPlan{Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}, Alias: sqlparser.NewTableIdent("t1")},
					Right: &AliasPlan{Input: &ScanPlan{Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t2")}}, Alias: sqlparser.NewTableIdent("t2")},
					Join:  sqlparser.JoinStr,
					On: &sqlparser.ComparisonExpr{
						Operator: sqlparser.EqualStr,
						Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("id"), Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
						Right:    &sqlparser.ColName{Name: sqlparser.NewColIdent("id"), Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("t2")}},
					},
				},
				Exprs: sqlparser.SelectExprs{&sqlparser.StarExpr{}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(sqlparser.String(tt.node), func(t *testing.T) {
			planner := NewPlanner()
			plan, err := planner.Plan(tt.node)
			require.NoError(t, err)
			require.Equal(t, tt.plan, plan)
		})
	}
}
