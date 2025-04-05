package task

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xwb1989/sqlparser"
)

func TestPartition(t *testing.T) {
	tests := []struct {
		node   sqlparser.SQLNode
		expect map[sqlparser.TableIdent]sqlparser.SQLNode
	}{
		{
			node: &sqlparser.Select{SelectExprs: sqlparser.SelectExprs{&sqlparser.StarExpr{}}, From: sqlparser.TableExprs{&sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}}},
			expect: map[sqlparser.TableIdent]sqlparser.SQLNode{
				{}: &sqlparser.Select{SelectExprs: sqlparser.SelectExprs{&sqlparser.StarExpr{}}, From: sqlparser.TableExprs{&sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}}},
			},
		},
	}

	for _, test := range tests {
		buf := sqlparser.NewTrackedBuffer(nil)
		test.node.Format(buf)
		t.Run(buf.String(), func(t *testing.T) {
			part := Partition(test.node)
			require.Equal(t, test.expect, part)
		})
	}
}
