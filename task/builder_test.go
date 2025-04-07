package task

import (
	"context"
	"database/sql/driver"
	"testing"
	"time"

	"github.com/siyul-park/sqlbridge/plan"
	"github.com/siyul-park/sqlbridge/schema"
	"github.com/siyul-park/sqlbridge/vm"
	"github.com/stretchr/testify/require"
	"github.com/xwb1989/sqlparser"
)

func TestTask_BuildAndRun(t *testing.T) {
	t1 := schema.NewInMemoryTable([]schema.Record{
		{
			Columns: []*sqlparser.ColName{
				{Name: sqlparser.NewColIdent("id")},
				{Name: sqlparser.NewColIdent("name")},
			},
			Values: []driver.Value{1, "foo"},
		},
	})
	t2 := schema.NewInMemoryTable([]schema.Record{
		{
			Columns: []*sqlparser.ColName{
				{Name: sqlparser.NewColIdent("id")},
				{Name: sqlparser.NewColIdent("name")},
			},
			Values: []driver.Value{2, "bar"},
		},
	})
	catalog := schema.NewInMemoryCatalog(map[string]schema.Table{
		"t1": t1,
		"t2": t2,
	})
	builder := NewBuilder(catalog)

	tests := []struct {
		plan  plan.Plan
		task  Task
		value driver.Value
	}{
		{
			plan:  &plan.NopPlan{},
			task:  &NopTask{},
			value: nil,
		},
		{
			plan: &plan.ScanPlan{Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
			task: &ScanTask{Catalog: catalog, Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
			value: schema.NewInMemoryCursor([]schema.Record{
				{
					Columns: []*sqlparser.ColName{
						{Name: sqlparser.NewColIdent("id")},
						{Name: sqlparser.NewColIdent("name")},
					},
					Values: []driver.Value{1, "foo"},
				},
			}),
		},
		{
			plan: &plan.AliasPlan{Input: &plan.ScanPlan{Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}, Alias: sqlparser.NewTableIdent("t1")},
			task: &AliasTask{Input: &ScanTask{Catalog: catalog, Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}, Alias: sqlparser.NewTableIdent("t1")},
			value: schema.NewInMemoryCursor([]schema.Record{
				{
					Columns: []*sqlparser.ColName{
						{Name: sqlparser.NewColIdent("id"), Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
						{Name: sqlparser.NewColIdent("name"), Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
					},
					Values: []driver.Value{1, "foo"},
				},
			}),
		},
		{
			plan: &plan.JoinPlan{
				Left:  &plan.AliasPlan{Input: &plan.ScanPlan{Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}, Alias: sqlparser.NewTableIdent("t1")},
				Right: &plan.AliasPlan{Input: &plan.ScanPlan{Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t2")}}, Alias: sqlparser.NewTableIdent("t2")},
				Join:  sqlparser.JoinStr,
			},
			task: &JoinTask{
				VM:    vm.New(),
				Left:  &AliasTask{Input: &ScanTask{Catalog: catalog, Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}, Alias: sqlparser.NewTableIdent("t1")},
				Right: &AliasTask{Input: &ScanTask{Catalog: catalog, Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t2")}}, Alias: sqlparser.NewTableIdent("t2")},
				Join:  sqlparser.JoinStr,
			},
			value: schema.NewInMemoryCursor([]schema.Record{
				{
					Columns: []*sqlparser.ColName{
						{Name: sqlparser.NewColIdent("id"), Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
						{Name: sqlparser.NewColIdent("name"), Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
						{Name: sqlparser.NewColIdent("id"), Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("t2")}},
						{Name: sqlparser.NewColIdent("name"), Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("t2")}},
					},
					Values: []driver.Value{1, "foo", 2, "bar"},
				},
			}),
		},
		{
			plan: &plan.JoinPlan{
				Left:  &plan.AliasPlan{Input: &plan.ScanPlan{Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}, Alias: sqlparser.NewTableIdent("t1")},
				Right: &plan.AliasPlan{Input: &plan.ScanPlan{Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t2")}}, Alias: sqlparser.NewTableIdent("t2")},
				Join:  sqlparser.JoinStr,
				On: &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("id"), Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
					Right:    &sqlparser.ColName{Name: sqlparser.NewColIdent("id"), Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("t2")}},
				},
			},
			task: &JoinTask{
				VM:    vm.New(),
				Left:  &AliasTask{Input: &ScanTask{Catalog: catalog, Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}, Alias: sqlparser.NewTableIdent("t1")},
				Right: &AliasTask{Input: &ScanTask{Catalog: catalog, Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t2")}}, Alias: sqlparser.NewTableIdent("t2")},
				Join:  sqlparser.JoinStr,
				On: &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("id"), Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
					Right:    &sqlparser.ColName{Name: sqlparser.NewColIdent("id"), Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("t2")}},
				},
			},
			value: schema.NewInMemoryCursor(nil),
		},
		{
			plan: &plan.JoinPlan{
				Left:  &plan.AliasPlan{Input: &plan.ScanPlan{Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}, Alias: sqlparser.NewTableIdent("t1")},
				Right: &plan.AliasPlan{Input: &plan.ScanPlan{Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t2")}}, Alias: sqlparser.NewTableIdent("t2")},
				Join:  sqlparser.JoinStr,
				Using: []sqlparser.ColIdent{sqlparser.NewColIdent("id")},
			},
			task: &JoinTask{
				VM:    vm.New(),
				Left:  &AliasTask{Input: &ScanTask{Catalog: catalog, Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}, Alias: sqlparser.NewTableIdent("t1")},
				Right: &AliasTask{Input: &ScanTask{Catalog: catalog, Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t2")}}, Alias: sqlparser.NewTableIdent("t2")},
				Join:  sqlparser.JoinStr,
				Using: []sqlparser.ColIdent{sqlparser.NewColIdent("id")},
			},
			value: schema.NewInMemoryCursor(nil),
		},
		{
			plan: &plan.FilterPlan{
				Input: &plan.AliasPlan{Input: &plan.ScanPlan{Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}, Alias: sqlparser.NewTableIdent("t1")},
				Expr: &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("id")},
					Right:    sqlparser.NewIntVal([]byte("0")),
				},
			},
			task: &FilterTask{
				VM:    vm.New(),
				Input: &AliasTask{Input: &ScanTask{Catalog: catalog, Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}, Alias: sqlparser.NewTableIdent("t1")},
				Expr: &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("id")},
					Right:    sqlparser.NewIntVal([]byte("0")),
				},
			},
			value: schema.NewInMemoryCursor(nil),
		},
		{
			plan: &plan.ProjectPlan{
				Input: &plan.AliasPlan{Input: &plan.ScanPlan{Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}, Alias: sqlparser.NewTableIdent("t1")},
				Exprs: sqlparser.SelectExprs{
					&sqlparser.AliasedExpr{Expr: &sqlparser.ColName{Name: sqlparser.NewColIdent("id")}},
				},
			},
			task: &ProjectTask{
				VM:    vm.New(),
				Input: &AliasTask{Input: &ScanTask{Catalog: catalog, Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}, Alias: sqlparser.NewTableIdent("t1")},
				Exprs: sqlparser.SelectExprs{
					&sqlparser.AliasedExpr{Expr: &sqlparser.ColName{Name: sqlparser.NewColIdent("id")}},
				},
			},
			value: schema.NewInMemoryCursor([]schema.Record{
				{
					Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("id")}},
					Values:  []driver.Value{1}},
			}),
		},
		{
			plan: &plan.ProjectPlan{
				Input: &plan.AliasPlan{Input: &plan.ScanPlan{Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}, Alias: sqlparser.NewTableIdent("t1")},
				Exprs: sqlparser.SelectExprs{&sqlparser.StarExpr{}},
			},
			task: &ProjectTask{
				VM:    vm.New(),
				Input: &AliasTask{Input: &ScanTask{Catalog: catalog, Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}, Alias: sqlparser.NewTableIdent("t1")},
				Exprs: sqlparser.SelectExprs{&sqlparser.StarExpr{}},
			},
			value: schema.NewInMemoryCursor([]schema.Record{
				{
					Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("id")}, {Name: sqlparser.NewColIdent("name")}},
					Values:  []driver.Value{1, "foo"},
				},
			}),
		},
		{
			plan: &plan.ProjectPlan{
				Input: &plan.AliasPlan{Input: &plan.ScanPlan{Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}, Alias: sqlparser.NewTableIdent("t1")},
				Exprs: sqlparser.SelectExprs{&sqlparser.StarExpr{TableName: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}},
			},
			task: &ProjectTask{
				VM:    vm.New(),
				Input: &AliasTask{Input: &ScanTask{Catalog: catalog, Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}, Alias: sqlparser.NewTableIdent("t1")},
				Exprs: sqlparser.SelectExprs{&sqlparser.StarExpr{TableName: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}},
			},
			value: schema.NewInMemoryCursor([]schema.Record{
				{
					Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("id")}, {Name: sqlparser.NewColIdent("name")}},
					Values:  []driver.Value{1, "foo"},
				},
			}),
		},
		{
			plan: &plan.GroupPlan{
				Input: &plan.AliasPlan{Input: &plan.ScanPlan{Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}, Alias: sqlparser.NewTableIdent("t1")},
				Exprs: sqlparser.GroupBy{&sqlparser.ColName{Name: sqlparser.NewColIdent("id")}},
			},
			task: &GroupTask{
				VM:    vm.New(),
				Input: &AliasTask{Input: &ScanTask{Catalog: catalog, Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}, Alias: sqlparser.NewTableIdent("t1")},
				Exprs: sqlparser.GroupBy{&sqlparser.ColName{Name: sqlparser.NewColIdent("id")}},
			},
			value: schema.NewInMemoryCursor([]schema.Record{
				{
					Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("id")}, schema.GroupColumn},
					Values: []driver.Value{1, []schema.Record{
						{
							Columns: []*sqlparser.ColName{
								{Name: sqlparser.NewColIdent("id"), Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
								{Name: sqlparser.NewColIdent("name"), Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
							},
							Values: []driver.Value{1, "foo"},
						},
					}},
				},
			}),
		},
		{
			plan: &plan.OrderPlan{
				Input: &plan.AliasPlan{Input: &plan.ScanPlan{Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}, Alias: sqlparser.NewTableIdent("t1")},
				Exprs: sqlparser.OrderBy{
					&sqlparser.Order{Expr: &sqlparser.ColName{Name: sqlparser.NewColIdent("id")}, Direction: sqlparser.DescScr},
				},
			},
			task: &OrderTask{
				VM:    vm.New(),
				Input: &AliasTask{Input: &ScanTask{Catalog: catalog, Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}, Alias: sqlparser.NewTableIdent("t1")},
				Exprs: sqlparser.OrderBy{
					&sqlparser.Order{Expr: &sqlparser.ColName{Name: sqlparser.NewColIdent("id")}, Direction: sqlparser.DescScr},
				},
			},
			value: schema.NewInMemoryCursor([]schema.Record{
				{
					Columns: []*sqlparser.ColName{
						{Name: sqlparser.NewColIdent("id"), Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
						{Name: sqlparser.NewColIdent("name"), Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
					},
					Values: []driver.Value{1, "foo"},
				},
			}),
		},
		{
			plan: &plan.LimitPlan{
				Input: &plan.AliasPlan{Input: &plan.ScanPlan{Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}, Alias: sqlparser.NewTableIdent("t1")},
				Exprs: &sqlparser.Limit{Offset: sqlparser.NewIntVal([]byte("0")), Rowcount: sqlparser.NewIntVal([]byte("1"))},
			},
			task: &LimitTask{
				VM:    vm.New(),
				Input: &AliasTask{Input: &ScanTask{Catalog: catalog, Table: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}, Alias: sqlparser.NewTableIdent("t1")},
				Exprs: &sqlparser.Limit{Offset: sqlparser.NewIntVal([]byte("0")), Rowcount: sqlparser.NewIntVal([]byte("1"))},
			},
			value: schema.NewInMemoryCursor([]schema.Record{
				{
					Columns: []*sqlparser.ColName{
						{Name: sqlparser.NewColIdent("id"), Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
						{Name: sqlparser.NewColIdent("name"), Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
					},
					Values: []driver.Value{1, "foo"},
				},
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.plan.String(), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			task, err := builder.Build(tt.plan)
			require.NoError(t, err)
			require.Equal(t, tt.task, task)

			val, err := task.Run(ctx)
			require.NoError(t, err)
			require.Equal(t, tt.value, val)
		})
	}
}
