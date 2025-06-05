package engine

import (
	"context"
	"testing"
	"time"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/stretchr/testify/require"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
)

func TestTupleExpr_Eval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	expr := &TupleExpr{
		Exprs: []Expr{
			&LiteralExpr{Value: sqltypes.NewInt64(1)},
			&LiteralExpr{Value: sqltypes.NewVarChar("foo")},
			&LiteralExpr{Value: sqltypes.NewFloat64(3.14)},
		},
	}

	expected := NewTuple([]Value{
		NewInt64(1),
		NewVarChar("foo"),
		NewFloat64(3.14),
	})

	actual, err := expr.Eval(ctx, schema.Row{}, nil)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestSpreadExpr_Eval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	expr := &SpreadExpr{
		Exprs: []Expr{
			&TupleExpr{
				Exprs: []Expr{
					&LiteralExpr{Value: sqltypes.NewInt64(1)},
					&LiteralExpr{Value: sqltypes.NewInt64(2)},
				},
			},
			&LiteralExpr{Value: sqltypes.NewInt64(3)},
		},
	}

	expected := NewTuple([]Value{
		NewInt64(1),
		NewInt64(2),
		NewInt64(3),
	})

	actual, err := expr.Eval(ctx, schema.Row{}, nil)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestIndexExpr_Eval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	tests := []struct {
		left     Expr
		right    Expr
		expected Value
	}{
		{
			left: &TupleExpr{
				Exprs: []Expr{
					&LiteralExpr{Value: sqltypes.NewInt64(1)},
					&LiteralExpr{Value: sqltypes.NewVarChar("foo")},
					&LiteralExpr{Value: sqltypes.NewFloat64(3.14)},
				},
			},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(1)},
			expected: NewVarChar("foo"),
		},
		{
			left: &TupleExpr{
				Exprs: []Expr{
					&LiteralExpr{Value: sqltypes.NewInt64(1)},
					&LiteralExpr{Value: sqltypes.NewVarChar("foo")},
					&LiteralExpr{Value: sqltypes.NewFloat64(3.14)},
				},
			},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(2)},
			expected: NewFloat64(3.14),
		},
	}

	for _, test := range tests {
		expr := &IndexExpr{Left: test.left, Right: test.right}
		t.Run(expr.String(), func(t *testing.T) {
			actual, err := expr.Eval(ctx, schema.Row{}, nil)
			require.NoError(t, err)
			require.Equal(t, test.expected, actual)
		})
	}
}
