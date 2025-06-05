package engine

import (
	"context"
	"testing"
	"time"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/stretchr/testify/require"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
)

func TestEqualExpr_Eval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	tests := []struct {
		left     Expr
		right    Expr
		expected Value
	}{
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(42)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(42)},
			expected: True,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(100)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(50)},
			expected: False,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewVarChar("hello")},
			right:    &LiteralExpr{Value: sqltypes.NewVarChar("hello")},
			expected: True,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewVarChar("foo")},
			right:    &LiteralExpr{Value: sqltypes.NewVarChar("bar")},
			expected: False,
		},
	}

	for _, tt := range tests {
		expr := &EqualExpr{Left: tt.left, Right: tt.right}
		t.Run(expr.String(), func(t *testing.T) {
			actual, err := expr.Eval(ctx, schema.Row{}, nil)
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual)
		})
	}
}

func TestGreaterThanExpr_Eval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	tests := []struct {
		left     Expr
		right    Expr
		expected Value
	}{
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(10)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(5)},
			expected: True,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(5)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(10)},
			expected: False,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewFloat64(3.5)},
			right:    &LiteralExpr{Value: sqltypes.NewFloat64(2.5)},
			expected: True,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewFloat64(2.5)},
			right:    &LiteralExpr{Value: sqltypes.NewFloat64(3.5)},
			expected: False,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewVarChar("b")},
			right:    &LiteralExpr{Value: sqltypes.NewVarChar("a")},
			expected: True,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewVarChar("a")},
			right:    &LiteralExpr{Value: sqltypes.NewVarChar("b")},
			expected: False,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(5)},
			right:    &LiteralExpr{Value: sqltypes.NewFloat64(3.0)},
			expected: True,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewFloat64(2.0)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(3)},
			expected: False,
		},
	}

	for _, tt := range tests {
		expr := &GreaterThanExpr{Left: tt.left, Right: tt.right}
		t.Run(expr.String(), func(t *testing.T) {
			actual, err := expr.Eval(ctx, schema.Row{}, nil)
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual)
		})
	}
}

func TestGreaterThanOrEqualExpr_Eval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	tests := []struct {
		left     Expr
		right    Expr
		expected Value
	}{
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(10)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(5)},
			expected: True,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(5)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(5)},
			expected: True,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(3)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(5)},
			expected: False,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewFloat64(3.5)},
			right:    &LiteralExpr{Value: sqltypes.NewFloat64(2.5)},
			expected: True,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewFloat64(2.5)},
			right:    &LiteralExpr{Value: sqltypes.NewFloat64(2.5)},
			expected: True,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewFloat64(2.0)},
			right:    &LiteralExpr{Value: sqltypes.NewFloat64(3.5)},
			expected: False,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewVarChar("b")},
			right:    &LiteralExpr{Value: sqltypes.NewVarChar("a")},
			expected: True,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewVarChar("a")},
			right:    &LiteralExpr{Value: sqltypes.NewVarChar("a")},
			expected: True,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewVarChar("a")},
			right:    &LiteralExpr{Value: sqltypes.NewVarChar("b")},
			expected: False,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(5)},
			right:    &LiteralExpr{Value: sqltypes.NewFloat64(3.0)},
			expected: True,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewFloat64(2.0)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(3)},
			expected: False,
		},
	}

	for _, tt := range tests {
		expr := &GreaterThanOrEqualExpr{Left: tt.left, Right: tt.right}
		t.Run(expr.String(), func(t *testing.T) {
			actual, err := expr.Eval(ctx, schema.Row{}, nil)
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual)
		})
	}
}

func TestLessThanExpr_Eval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	tests := []struct {
		left     Expr
		right    Expr
		expected Value
	}{
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(3)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(5)},
			expected: True,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(5)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(5)},
			expected: False,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(10)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(5)},
			expected: False,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewFloat64(2.5)},
			right:    &LiteralExpr{Value: sqltypes.NewFloat64(3.5)},
			expected: True,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewFloat64(3.5)},
			right:    &LiteralExpr{Value: sqltypes.NewFloat64(3.5)},
			expected: False,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewFloat64(4.0)},
			right:    &LiteralExpr{Value: sqltypes.NewFloat64(3.5)},
			expected: False,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewVarChar("a")},
			right:    &LiteralExpr{Value: sqltypes.NewVarChar("b")},
			expected: True,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewVarChar("b")},
			right:    &LiteralExpr{Value: sqltypes.NewVarChar("b")},
			expected: False,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewVarChar("c")},
			right:    &LiteralExpr{Value: sqltypes.NewVarChar("b")},
			expected: False,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(3)},
			right:    &LiteralExpr{Value: sqltypes.NewFloat64(4.0)},
			expected: True,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewFloat64(5.0)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(3)},
			expected: False,
		},
	}

	for _, tt := range tests {
		expr := &LessThanExpr{Left: tt.left, Right: tt.right}
		t.Run(expr.String(), func(t *testing.T) {
			actual, err := expr.Eval(ctx, schema.Row{}, nil)
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual)
		})
	}
}

func TestLessThanOrEqualExpr_Eval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	tests := []struct {
		left     Expr
		right    Expr
		expected Value
	}{
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(3)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(5)},
			expected: True,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(5)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(5)},
			expected: True,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(10)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(5)},
			expected: False,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewFloat64(2.5)},
			right:    &LiteralExpr{Value: sqltypes.NewFloat64(3.5)},
			expected: True,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewFloat64(3.5)},
			right:    &LiteralExpr{Value: sqltypes.NewFloat64(3.5)},
			expected: True,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewFloat64(4.0)},
			right:    &LiteralExpr{Value: sqltypes.NewFloat64(3.5)},
			expected: False,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewVarChar("a")},
			right:    &LiteralExpr{Value: sqltypes.NewVarChar("b")},
			expected: True,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewVarChar("b")},
			right:    &LiteralExpr{Value: sqltypes.NewVarChar("b")},
			expected: True,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewVarChar("c")},
			right:    &LiteralExpr{Value: sqltypes.NewVarChar("b")},
			expected: False,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(3)},
			right:    &LiteralExpr{Value: sqltypes.NewFloat64(4.0)},
			expected: True,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewFloat64(5.0)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(3)},
			expected: False,
		},
	}

	for _, tt := range tests {
		expr := &LessThanOrEqualExpr{Left: tt.left, Right: tt.right}
		t.Run(expr.String(), func(t *testing.T) {
			actual, err := expr.Eval(ctx, schema.Row{}, nil)
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual)
		})
	}
}

func TestInExpr_Eval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	tests := []struct {
		left     Expr
		right    Expr
		expected Value
	}{
		{
			left: &LiteralExpr{Value: sqltypes.NewInt64(3)},
			right: &TupleExpr{
				Exprs: []Expr{
					&LiteralExpr{Value: sqltypes.NewInt64(1)},
					&LiteralExpr{Value: sqltypes.NewInt64(3)},
					&LiteralExpr{Value: sqltypes.NewInt64(5)},
				},
			},
			expected: True,
		},
		{
			left: &LiteralExpr{Value: sqltypes.NewInt64(2)},
			right: &TupleExpr{
				Exprs: []Expr{
					&LiteralExpr{Value: sqltypes.NewInt64(1)},
					&LiteralExpr{Value: sqltypes.NewInt64(3)},
					&LiteralExpr{Value: sqltypes.NewInt64(5)},
				},
			},
			expected: False,
		},
		{
			left: &LiteralExpr{Value: sqltypes.NewVarChar("foo")},
			right: &TupleExpr{
				Exprs: []Expr{
					&LiteralExpr{Value: sqltypes.NewVarChar("foo")},
					&LiteralExpr{Value: sqltypes.NewVarChar("bar")},
				},
			},
			expected: True,
		},
		{
			left: &LiteralExpr{Value: sqltypes.NewVarChar("baz")},
			right: &TupleExpr{
				Exprs: []Expr{
					&LiteralExpr{Value: sqltypes.NewVarChar("foo")},
					&LiteralExpr{Value: sqltypes.NewVarChar("bar")},
				},
			},
			expected: False,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(1)},
			right:    &TupleExpr{},
			expected: False,
		},
	}

	for _, tt := range tests {
		expr := &InExpr{Left: tt.left, Right: tt.right}
		t.Run(expr.String(), func(t *testing.T) {
			actual, err := expr.Eval(ctx, schema.Row{}, nil)
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual)
		})
	}
}

func TestMatchExpr_Eval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	tests := []struct {
		left     Expr
		right    Expr
		expected Value
	}{
		{
			left:     &LiteralExpr{Value: sqltypes.NewVarChar("hello world")},
			right:    &LiteralExpr{Value: sqltypes.NewVarChar("world")},
			expected: True,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewVarChar("hello world")},
			right:    &LiteralExpr{Value: sqltypes.NewVarChar("WORLD")},
			expected: False,
		},
		{
			left: &TupleExpr{
				Exprs: []Expr{
					&LiteralExpr{Value: sqltypes.NewVarChar("foo")},
					&LiteralExpr{Value: sqltypes.NewVarChar("bar")},
					&LiteralExpr{Value: sqltypes.NewVarChar("baz")},
				},
			},
			right:    &LiteralExpr{Value: sqltypes.NewVarChar("ar")},
			expected: True,
		},
		{
			left: &TupleExpr{
				Exprs: []Expr{
					&LiteralExpr{Value: sqltypes.NewVarChar("foo")},
					&LiteralExpr{Value: sqltypes.NewVarChar("bar")},
					&LiteralExpr{Value: sqltypes.NewVarChar("baz")},
				},
			},
			right:    &LiteralExpr{Value: sqltypes.NewVarChar("qux")},
			expected: False,
		},
		{
			left:     &TupleExpr{},
			right:    &LiteralExpr{Value: sqltypes.NewVarChar("any")},
			expected: False,
		},
	}

	for _, tt := range tests {
		expr := &MatchExpr{Left: tt.left, Right: tt.right}
		t.Run(expr.String(), func(t *testing.T) {
			actual, err := expr.Eval(ctx, schema.Row{}, nil)
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual)
		})
	}
}

func TestLikeExpr_Eval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	tests := []struct {
		left     Expr
		right    Expr
		expected Value
	}{
		{
			left:     &LiteralExpr{Value: sqltypes.NewVarChar("hello")},
			right:    &LiteralExpr{Value: sqltypes.NewVarChar("h%o")},
			expected: True,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewVarChar("hello")},
			right:    &LiteralExpr{Value: sqltypes.NewVarChar("he__o")},
			expected: True,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewVarChar("hello")},
			right:    &LiteralExpr{Value: sqltypes.NewVarChar("he__")},
			expected: False,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewVarChar("hello")},
			right:    &LiteralExpr{Value: sqltypes.NewVarChar("%ell%")},
			expected: True,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewVarChar("a.c")},
			right:    &LiteralExpr{Value: sqltypes.NewVarChar("a.c")},
			expected: True,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewVarChar("abc")},
			right:    &LiteralExpr{Value: sqltypes.NewVarChar("a.c")},
			expected: False,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewVarChar("abc")},
			right:    &LiteralExpr{Value: sqltypes.NewVarChar("a_d")},
			expected: False,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewVarChar("")},
			right:    &LiteralExpr{Value: sqltypes.NewVarChar("")},
			expected: True,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewVarChar("")},
			right:    &LiteralExpr{Value: sqltypes.NewVarChar("%")},
			expected: True,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewVarChar("")},
			right:    &LiteralExpr{Value: sqltypes.NewVarChar("_")},
			expected: False,
		},
	}

	for _, tt := range tests {
		expr := &LikeExpr{Left: tt.left, Right: tt.right}
		t.Run(expr.String(), func(t *testing.T) {
			actual, err := expr.Eval(ctx, schema.Row{}, nil)
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual)
		})
	}
}

func TestRegexpExpr_Eval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	tests := []struct {
		left     Expr
		right    Expr
		expected Value
	}{
		{
			left:     &LiteralExpr{Value: sqltypes.NewVarChar("hello123")},
			right:    &LiteralExpr{Value: sqltypes.NewVarChar(`hello\d+`)},
			expected: True,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewVarChar("hello")},
			right:    &LiteralExpr{Value: sqltypes.NewVarChar(`^h.*o$`)},
			expected: True,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewVarChar("hello")},
			right:    &LiteralExpr{Value: sqltypes.NewVarChar(`^H.*o$`)},
			expected: False,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewVarChar("tt")},
			right:    &LiteralExpr{Value: sqltypes.NewVarChar(`^abc`)},
			expected: False,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewVarChar("")},
			right:    &LiteralExpr{Value: sqltypes.NewVarChar(`^$`)},
			expected: True,
		},
	}

	for _, tt := range tests {
		expr := &RegexpExpr{Left: tt.left, Right: tt.right}
		t.Run(expr.String(), func(t *testing.T) {
			actual, err := expr.Eval(ctx, schema.Row{}, nil)
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual)
		})
	}
}

func TestIdenticalExpr_Eval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	tests := []struct {
		input    Expr
		expected Value
	}{
		{
			input:    &LiteralExpr{Value: sqltypes.NewInt64(3)},
			expected: True,
		},
		{
			input: &TupleExpr{
				Exprs: []Expr{
					&LiteralExpr{Value: sqltypes.NewInt64(1)},
					&LiteralExpr{Value: sqltypes.NewInt64(3)},
					&LiteralExpr{Value: sqltypes.NewInt64(5)},
				},
			},
			expected: False,
		},
		{
			input: &TupleExpr{
				Exprs: []Expr{
					&LiteralExpr{Value: sqltypes.NewInt64(3)},
					&LiteralExpr{Value: sqltypes.NewInt64(3)},
					&LiteralExpr{Value: sqltypes.NewInt64(3)},
				},
			},
			expected: True,
		},
	}

	for _, tt := range tests {
		e := &IdenticalExpr{Input: tt.input}
		got, err := e.Eval(ctx, schema.Row{}, nil)
		require.NoError(t, err)
		require.Equal(t, tt.expected, got)
	}
}
