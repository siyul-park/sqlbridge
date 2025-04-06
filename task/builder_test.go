package task

import (
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/siyul-park/sqlbridge/plan"
	"github.com/siyul-park/sqlbridge/schema"
	"github.com/stretchr/testify/require"
	"github.com/xwb1989/sqlparser"
	"testing"
	"time"
)

func TestTask_Run(t *testing.T) {
	t1 := schema.NewInMemoryTable([][]string{{"id", "name"}}, [][]driver.Value{{1, "foo"}})
	t2 := schema.NewInMemoryTable([][]string{{"id", "name"}}, [][]driver.Value{{2, "bar"}})

	builder := NewBuilder()

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
			plan:  &plan.ScanPlan{Table: t1},
			task:  &ScanTask{Table: t1},
			value: schema.NewInMemoryRows([][]string{{"id", "name"}}, [][]driver.Value{{1, "foo"}}),
		},
		{
			plan:  &plan.AliasPlan{Input: &plan.ScanPlan{Table: t1}, Alias: sqlparser.NewTableIdent("t1")},
			task:  &AliasTask{Input: &ScanTask{Table: t1}, Alias: sqlparser.NewTableIdent("t1")},
			value: schema.NewInMemoryRows([][]string{{"t1.id", "t1.name"}}, [][]driver.Value{{1, "foo"}}),
		},
		{
			plan: &plan.JoinPlan{
				Left:  &plan.AliasPlan{Input: &plan.ScanPlan{Table: t1}, Alias: sqlparser.NewTableIdent("t1")},
				Right: &plan.AliasPlan{Input: &plan.ScanPlan{Table: t2}, Alias: sqlparser.NewTableIdent("t2")},
			},
			task: &JoinTask{
				Left:  &AliasTask{Input: &ScanTask{Table: t1}, Alias: sqlparser.NewTableIdent("t1")},
				Right: &AliasTask{Input: &ScanTask{Table: t2}, Alias: sqlparser.NewTableIdent("t2")},
			},
			value: schema.NewInMemoryRows([][]string{{"t1.id", "t1.name", "t2.id", "t2.name"}}, [][]driver.Value{{1, "foo", 2, "bar"}}),
		},
		{
			plan: &plan.JoinPlan{
				Left:  &plan.AliasPlan{Input: &plan.ScanPlan{Table: t1}, Alias: sqlparser.NewTableIdent("t1")},
				Right: &plan.AliasPlan{Input: &plan.ScanPlan{Table: t2}, Alias: sqlparser.NewTableIdent("t2")},
				On: &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("id"), Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
					Right:    &sqlparser.ColName{Name: sqlparser.NewColIdent("id"), Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("t2")}},
				},
			},
			task: &JoinTask{
				Left:  &AliasTask{Input: &ScanTask{Table: t1}, Alias: sqlparser.NewTableIdent("t1")},
				Right: &AliasTask{Input: &ScanTask{Table: t2}, Alias: sqlparser.NewTableIdent("t2")},
				On: &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("id"), Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
					Right:    &sqlparser.ColName{Name: sqlparser.NewColIdent("id"), Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("t2")}},
				},
			},
			value: schema.NewInMemoryRows(nil, nil),
		},
		{
			plan: &plan.JoinPlan{
				Left:  &plan.AliasPlan{Input: &plan.ScanPlan{Table: t1}, Alias: sqlparser.NewTableIdent("t1")},
				Right: &plan.AliasPlan{Input: &plan.ScanPlan{Table: t2}, Alias: sqlparser.NewTableIdent("t2")},
				Using: []sqlparser.ColIdent{sqlparser.NewColIdent("id")},
			},
			task: &JoinTask{
				Left:  &AliasTask{Input: &ScanTask{Table: t1}, Alias: sqlparser.NewTableIdent("t1")},
				Right: &AliasTask{Input: &ScanTask{Table: t2}, Alias: sqlparser.NewTableIdent("t2")},
				Using: []sqlparser.ColIdent{sqlparser.NewColIdent("id")},
			},
			value: schema.NewInMemoryRows(nil, nil),
		},
		{
			plan: &plan.FilterPlan{
				Input: &plan.AliasPlan{
					Input: &plan.ScanPlan{Table: t1},
					Alias: sqlparser.NewTableIdent("t1"),
				},
				Expr: &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("id")},
					Right:    sqlparser.NewIntVal([]byte("0")),
				},
			},
			task: &FilterTask{
				Input: &AliasTask{Input: &ScanTask{Table: t1}, Alias: sqlparser.NewTableIdent("t1")},
				Expr: &sqlparser.ComparisonExpr{
					Operator: sqlparser.EqualStr,
					Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("id")},
					Right:    sqlparser.NewIntVal([]byte("0")),
				},
			},
			value: schema.NewInMemoryRows(nil, nil),
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%v", tt.plan), func(t *testing.T) {
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
