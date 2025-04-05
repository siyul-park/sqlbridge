package task

import (
	"context"
	"database/sql/driver"
	"testing"
	"time"

	"github.com/siyul-park/sqlbridge/schema"

	"github.com/stretchr/testify/require"
	"github.com/xwb1989/sqlparser"
)

func TestSelectBuilder_Build(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	registry := NewRegistry()
	registry.AddBuilder(NewSelectTask(registry))
	registry.AddBuilder(NewTableBuilder(registry))
	registry.AddBuilder(NewExpressionBuilder(registry))

	tests := []struct {
		node   sqlparser.SQLNode
		value  any
		expect any
	}{
		{
			node: &sqlparser.Select{
				SelectExprs: sqlparser.SelectExprs{&sqlparser.AliasedExpr{Expr: &sqlparser.ColName{Name: sqlparser.NewColIdent("foo")}}},
				From:        sqlparser.TableExprs{&sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}},
				Where: &sqlparser.Where{
					Type: sqlparser.WhereStr,
					Expr: &sqlparser.ComparisonExpr{Left: &sqlparser.ColName{Name: sqlparser.NewColIdent("foo")}, Operator: sqlparser.EqualStr, Right: sqlparser.NewStrVal([]byte("bar"))},
				},
			},
			value:  schema.New(map[string]schema.Table{"t1": schema.NewInlineTable([][]string{{"foo"}}, [][]driver.Value{{"bar"}})}),
			expect: schema.NewInlineRows([][]string{{"foo"}}, [][]driver.Value{{"bar"}}),
		},
		{
			node: &sqlparser.Select{
				SelectExprs: sqlparser.SelectExprs{
					&sqlparser.AliasedExpr{Expr: &sqlparser.ColName{Name: sqlparser.NewColIdent("foo")}},
					&sqlparser.AliasedExpr{Expr: &sqlparser.ColName{Name: sqlparser.NewColIdent("bar")}},
				},
				From: sqlparser.TableExprs{
					&sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
					&sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: sqlparser.NewTableIdent("t2")}},
				},
				Where: &sqlparser.Where{
					Type: sqlparser.WhereStr,
					Expr: &sqlparser.AndExpr{
						Left:  &sqlparser.ComparisonExpr{Left: &sqlparser.ColName{Name: sqlparser.NewColIdent("foo")}, Operator: sqlparser.EqualStr, Right: sqlparser.NewStrVal([]byte("foo"))},
						Right: &sqlparser.ComparisonExpr{Left: &sqlparser.ColName{Name: sqlparser.NewColIdent("bar")}, Operator: sqlparser.EqualStr, Right: sqlparser.NewStrVal([]byte("bar"))},
					},
				},
			},
			value: schema.New(map[string]schema.Table{
				"t1": schema.NewInlineTable([][]string{{"foo"}}, [][]driver.Value{{"foo"}}),
				"t2": schema.NewInlineTable([][]string{{"bar"}}, [][]driver.Value{{"bar"}}),
			}),
			expect: schema.NewInlineRows([][]string{{"foo", "bar"}}, [][]driver.Value{{"foo", "bar"}}),
		},
	}

	for _, test := range tests {
		t.Run(sqlparser.String(test.node), func(t *testing.T) {
			task, err := registry.Build(test.node)
			require.NoError(t, err)

			result, err := task.Run(ctx, test.value)
			require.NoError(t, err)
			require.Equal(t, test.expect, result)
		})
	}
}
