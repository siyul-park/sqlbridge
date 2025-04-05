package task

import (
	"context"
	"database/sql/driver"
	"github.com/siyul-park/sqlbridge/schema"
	"github.com/stretchr/testify/require"
	"github.com/xwb1989/sqlparser"
	"testing"
	"time"
)

func TestSelectBuilder_Build(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	registry := NewRegistry()
	registry.AddBuilder(NewSelectTask(registry))
	registry.AddBuilder(NewTableBuilder(registry))

	tests := []struct {
		node   sqlparser.SQLNode
		value  any
		expect any
	}{
		{
			node: &sqlparser.Select{
				SelectExprs: sqlparser.SelectExprs{&sqlparser.StarExpr{}},
				From:        sqlparser.TableExprs{&sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}},
			},
			value:  schema.New(map[string]schema.Table{"t1": schema.NewTable([]map[string]driver.Value{{"foo": "bar"}})}),
			expect: schema.NewRows([]map[string]driver.Value{{"foo": "bar"}}),
		},
	}

	for _, test := range tests {
		buf := sqlparser.NewTrackedBuffer(nil)
		test.node.Format(buf)
		t.Run(buf.String(), func(t *testing.T) {
			task, err := registry.Build(test.node)
			require.NoError(t, err)

			result, err := task.Run(ctx, test.value)
			require.NoError(t, err)
			require.Equal(t, test.expect, result)
		})
	}
}
