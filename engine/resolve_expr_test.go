package engine

import (
	"context"
	"testing"
	"time"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/stretchr/testify/require"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/querypb"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
)

func TestValArgExpr_Eval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	tests := []struct {
		value string
		binds map[string]*querypb.BindVariable
	}{
		{
			value: "a1",
			binds: map[string]*querypb.BindVariable{
				"a1": {
					Type:  querypb.Type_INT64,
					Value: []byte("42"),
				},
			},
		},
	}

	for _, tt := range tests {
		expr := &ValArgExpr{Value: tt.value}
		t.Run(expr.String(), func(t *testing.T) {
			actual, err := expr.Eval(ctx, schema.Row{}, tt.binds)
			require.NoError(t, err)
			require.Equal(t, NewInt64(42), actual)
		})
	}
}

func TestColumnExpr_Eval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	tests := []struct {
		value *sqlparser.ColName
		row   schema.Row
	}{
		{
			value: &sqlparser.ColName{Name: sqlparser.NewColIdent("id")},
			row: schema.Row{
				Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("id")}, {Name: sqlparser.NewColIdent("name")}},
				Values:  []sqltypes.Value{sqltypes.NewInt64(0), sqltypes.MakeTrusted(sqltypes.VarChar, []byte("foo"))},
			},
		},
	}

	for _, tt := range tests {
		expr := &ColumnExpr{Value: tt.value}
		t.Run(expr.String(), func(t *testing.T) {
			actual, err := expr.Eval(ctx, tt.row, nil)
			require.NoError(t, err)
			require.Equal(t, NewTuple([]Value{NewInt64(0)}), actual)
		})
	}
}

func TestTableExpr_Eval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	tests := []struct {
		value sqlparser.TableName
		row   schema.Row
	}{
		{
			value: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")},
			row: schema.Row{
				Columns: []*sqlparser.ColName{
					{Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}, Name: sqlparser.NewColIdent("id")},
					{Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("t1")}, Name: sqlparser.NewColIdent("name")}},
				Values: []sqltypes.Value{sqltypes.NewInt64(0), sqltypes.MakeTrusted(sqltypes.VarChar, []byte("foo"))},
			},
		},
	}

	for _, tt := range tests {
		expr := &TableExpr{Value: tt.value}
		t.Run(expr.String(), func(t *testing.T) {
			actual, err := expr.Eval(ctx, tt.row, nil)
			require.NoError(t, err)
			require.Equal(t, NewTuple([]Value{NewInt64(0), NewVarChar("foo")}), actual)
		})
	}
}
