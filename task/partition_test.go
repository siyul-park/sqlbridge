package task

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xwb1989/sqlparser"
)

func TestPartition(t *testing.T) {
	tests := []struct {
		node   sqlparser.SQLNode
		expect map[sqlparser.TableName]sqlparser.SQLNode
	}{
		{
			node: &sqlparser.Select{SelectExprs: sqlparser.SelectExprs{&sqlparser.StarExpr{}}, From: sqlparser.TableExprs{&sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}}},
			expect: map[sqlparser.TableName]sqlparser.SQLNode{
				sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}: &sqlparser.Select{SelectExprs: sqlparser.SelectExprs{&sqlparser.StarExpr{}}, From: sqlparser.TableExprs{&sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}}},
			},
		},
		{
			node: sqlparser.SelectExprs{
				&sqlparser.AliasedExpr{Expr: &sqlparser.ColName{Name: sqlparser.NewColIdent("foo"), Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}},
				&sqlparser.AliasedExpr{Expr: &sqlparser.ColName{Name: sqlparser.NewColIdent("bar"), Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("t2")}}},
			},
			expect: map[sqlparser.TableName]sqlparser.SQLNode{
				sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}: sqlparser.SelectExprs{&sqlparser.AliasedExpr{Expr: &sqlparser.ColName{Name: sqlparser.NewColIdent("foo")}}},
				sqlparser.TableName{Name: sqlparser.NewTableIdent("t2")}: sqlparser.SelectExprs{&sqlparser.AliasedExpr{Expr: &sqlparser.ColName{Name: sqlparser.NewColIdent("bar")}}},
			},
		},
		{
			node: &sqlparser.AliasedExpr{Expr: &sqlparser.ColName{Name: sqlparser.NewColIdent("foo"), Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}},
			expect: map[sqlparser.TableName]sqlparser.SQLNode{
				sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}: &sqlparser.AliasedExpr{Expr: &sqlparser.ColName{Name: sqlparser.NewColIdent("foo")}},
			},
		},
		{
			node: sqlparser.TableExprs{
				&sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}, As: sqlparser.NewTableIdent("t3")},
				&sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: sqlparser.NewTableIdent("t2")}, As: sqlparser.NewTableIdent("t4")},
			},
			expect: map[sqlparser.TableName]sqlparser.SQLNode{
				sqlparser.TableName{Name: sqlparser.NewTableIdent("t3")}: sqlparser.TableExprs{&sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}}},
				sqlparser.TableName{Name: sqlparser.NewTableIdent("t4")}: sqlparser.TableExprs{&sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: sqlparser.NewTableIdent("t2")}}},
			},
		},
		{
			node: &sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}, As: sqlparser.NewTableIdent("t2")},
			expect: map[sqlparser.TableName]sqlparser.SQLNode{
				sqlparser.TableName{Name: sqlparser.NewTableIdent("t2")}: &sqlparser.AliasedTableExpr{Expr: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
			},
		},
		{
			node: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")},
			expect: map[sqlparser.TableName]sqlparser.SQLNode{
				sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")},
			},
		},
		{
			node: &sqlparser.ColName{Name: sqlparser.NewColIdent("foo"), Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}},
			expect: map[sqlparser.TableName]sqlparser.SQLNode{
				sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}: &sqlparser.ColName{Name: sqlparser.NewColIdent("foo")},
			},
		},
		{
			node: sqlparser.NewTableIdent("t1"),
			expect: map[sqlparser.TableName]sqlparser.SQLNode{
				sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}: sqlparser.NewTableIdent("t1"),
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
