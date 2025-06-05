package engine

import (
	"context"
	"testing"
	"time"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/stretchr/testify/require"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
)

func TestAddExpr_Eval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	tests := []struct {
		left     Expr
		right    Expr
		expected Value
	}{
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(1)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(2)},
			expected: NewInt64(3),
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewFloat64(1.5)},
			right:    &LiteralExpr{Value: sqltypes.NewFloat64(2.5)},
			expected: NewFloat64(4.0),
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(1)},
			right:    &LiteralExpr{Value: sqltypes.NewFloat64(2.5)},
			expected: NewFloat64(3.5),
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewFloat64(3.5)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(2)},
			expected: NewFloat64(5.5),
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewVarChar("foo")},
			right:    &LiteralExpr{Value: sqltypes.NewVarChar("bar")},
			expected: NewVarChar("foobar"),
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewVarChar("count: ")},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(5)},
			expected: NewVarChar("count: 5"),
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(7)},
			right:    &LiteralExpr{Value: sqltypes.NewVarChar(" apples")},
			expected: NewVarChar("7 apples"),
		},
	}

	for _, tt := range tests {
		expr := &AddExpr{Left: tt.left, Right: tt.right}
		t.Run(expr.String(), func(t *testing.T) {
			actual, err := expr.Eval(ctx, schema.Row{}, nil)
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual)
		})
	}
}

func TestSubExpr_Eval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	tests := []struct {
		left     Expr
		right    Expr
		expected Value
	}{
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(5)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(3)},
			expected: NewInt64(2),
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewFloat64(5.5)},
			right:    &LiteralExpr{Value: sqltypes.NewFloat64(2.0)},
			expected: NewFloat64(3.5),
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(10)},
			right:    &LiteralExpr{Value: sqltypes.NewFloat64(4.5)},
			expected: NewFloat64(5.5),
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewFloat64(9.5)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(4)},
			expected: NewFloat64(5.5),
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(-1)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(-3)},
			expected: NewInt64(2),
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewFloat64(-1.5)},
			right:    &LiteralExpr{Value: sqltypes.NewFloat64(0.5)},
			expected: NewFloat64(-2.0),
		},
	}

	for _, test := range tests {
		expr := &SubExpr{Left: test.left, Right: test.right}
		t.Run(expr.String(), func(t *testing.T) {
			actual, err := expr.Eval(ctx, schema.Row{}, nil)
			require.NoError(t, err)
			require.Equal(t, test.expected, actual)
		})
	}
}

func TestMulExpr_Eval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	tests := []struct {
		left     Expr
		right    Expr
		expected Value
	}{
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(2)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(3)},
			expected: NewInt64(6),
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewFloat64(2.5)},
			right:    &LiteralExpr{Value: sqltypes.NewFloat64(4.0)},
			expected: NewFloat64(10.0),
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(2)},
			right:    &LiteralExpr{Value: sqltypes.NewFloat64(3.5)},
			expected: NewFloat64(7.0),
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewFloat64(1.5)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(4)},
			expected: NewFloat64(6.0),
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(0)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(100)},
			expected: NewInt64(0),
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewFloat64(-2.0)},
			right:    &LiteralExpr{Value: sqltypes.NewFloat64(3.5)},
			expected: NewFloat64(-7.0),
		},
	}

	for _, tt := range tests {
		expr := &MulExpr{Left: tt.left, Right: tt.right}
		t.Run(expr.String(), func(t *testing.T) {
			actual, err := expr.Eval(ctx, schema.Row{}, nil)
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual)
		})
	}
}

func TestDivExpr_Eval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	tests := []struct {
		left     Expr
		right    Expr
		expected Value
	}{
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(10)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(2)},
			expected: NewInt64(5),
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewFloat64(7.5)},
			right:    &LiteralExpr{Value: sqltypes.NewFloat64(2.5)},
			expected: NewFloat64(3.0),
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(7)},
			right:    &LiteralExpr{Value: sqltypes.NewFloat64(2)},
			expected: NewFloat64(3.5),
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewFloat64(9)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(3)},
			expected: NewFloat64(3.0),
		},
	}

	for _, tt := range tests {
		expr := &DivExpr{Left: tt.left, Right: tt.right}
		t.Run(expr.String(), func(t *testing.T) {
			actual, err := expr.Eval(ctx, schema.Row{}, nil)
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual)
		})
	}
}

func TestModExpr_Eval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	tests := []struct {
		left     Expr
		right    Expr
		expected Value
	}{
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(10)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(3)},
			expected: NewInt64(1),
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(14)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(7)},
			expected: NewInt64(0),
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(5)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(2)},
			expected: NewInt64(1),
		},
	}

	for _, tt := range tests {
		expr := &ModExpr{Left: tt.left, Right: tt.right}
		t.Run(expr.String(), func(t *testing.T) {
			actual, err := expr.Eval(ctx, schema.Row{}, nil)
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual)
		})
	}
}

func TestShiftLeftExpr_Eval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	tests := []struct {
		left     Expr
		right    Expr
		expected Value
	}{
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(1)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(1)},
			expected: NewInt64(2),
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(3)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(2)},
			expected: NewInt64(12),
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(7)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(0)},
			expected: NewInt64(7),
		},
	}

	for _, tt := range tests {
		expr := &ShiftLeftExpr{Left: tt.left, Right: tt.right}
		t.Run(expr.String(), func(t *testing.T) {
			actual, err := expr.Eval(ctx, schema.Row{}, nil)
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual)
		})
	}
}

func TestShiftRightExpr_Eval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	tests := []struct {
		left     Expr
		right    Expr
		expected Value
	}{
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(4)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(1)},
			expected: NewInt64(2),
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(16)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(3)},
			expected: NewInt64(2),
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(7)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(0)},
			expected: NewInt64(7),
		},
	}

	for _, tt := range tests {
		expr := &ShiftRightExpr{Left: tt.left, Right: tt.right}
		t.Run(expr.String(), func(t *testing.T) {
			actual, err := expr.Eval(ctx, schema.Row{}, nil)
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual)
		})
	}
}

func TestBitNotExpr_Eval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	tests := []struct {
		operand  Expr
		expected Value
	}{
		{
			operand:  &LiteralExpr{Value: sqltypes.NewInt64(0)},
			expected: NewInt64(^int64(0)),
		},
		{
			operand:  &LiteralExpr{Value: sqltypes.NewInt64(1)},
			expected: NewInt64(^int64(1)),
		},
		{
			operand:  &LiteralExpr{Value: sqltypes.NewInt64(15)},
			expected: NewInt64(^int64(15)),
		},
		{
			operand:  &LiteralExpr{Value: sqltypes.NewInt64(-1)},
			expected: NewInt64(^int64(-1)),
		},
	}

	for _, tt := range tests {
		expr := &BitNotExpr{Input: tt.operand}
		t.Run(expr.String(), func(t *testing.T) {
			actual, err := expr.Eval(ctx, schema.Row{}, nil)
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual)
		})
	}
}
