package engine

import (
	"context"
	"testing"
	"time"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/stretchr/testify/require"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
)

func TestAndExpr_Eval(t *testing.T) {
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
			expected: True,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(0)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(1)},
			expected: False,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(1)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(0)},
			expected: False,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(0)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(0)},
			expected: False,
		},
	}

	for _, tt := range tests {
		expr := &AndExpr{Left: tt.left, Right: tt.right}
		t.Run(expr.String(), func(t *testing.T) {
			actual, err := expr.Eval(ctx, schema.Row{}, nil)
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual)
		})
	}
}

func TestOrExpr_Eval(t *testing.T) {
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
			expected: True,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(0)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(1)},
			expected: True,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(1)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(0)},
			expected: True,
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewInt64(0)},
			right:    &LiteralExpr{Value: sqltypes.NewInt64(0)},
			expected: False,
		},
	}

	for _, tt := range tests {
		expr := &OrExpr{Left: tt.left, Right: tt.right}
		t.Run(expr.String(), func(t *testing.T) {
			actual, err := expr.Eval(ctx, schema.Row{}, nil)
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual)
		})
	}
}

func TestNotExpr_Eval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	tests := []struct {
		input    Expr
		expected Value
	}{
		{
			input:    &LiteralExpr{Value: sqltypes.NewInt64(1)},
			expected: False,
		},
		{
			input:    &LiteralExpr{Value: sqltypes.NewInt64(0)},
			expected: True,
		},
	}

	for _, tt := range tests {
		expr := &NotExpr{Input: tt.input}
		t.Run(expr.String(), func(t *testing.T) {
			actual, err := expr.Eval(ctx, schema.Row{}, nil)
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual)
		})
	}
}

func TestIfExpr_Eval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	tests := []struct {
		when     Expr
		then     Expr
		els      Expr
		expected Value
	}{
		{
			when:     &LiteralExpr{Value: sqltypes.NewInt64(1)},
			then:     &LiteralExpr{Value: sqltypes.NewInt64(42)},
			els:      &LiteralExpr{Value: sqltypes.NewInt64(0)},
			expected: NewInt64(42),
		},
		{
			when:     &LiteralExpr{Value: sqltypes.NewInt64(0)},
			then:     &LiteralExpr{Value: sqltypes.NewInt64(42)},
			els:      &LiteralExpr{Value: sqltypes.NewInt64(0)},
			expected: NewInt64(0),
		},
		{
			when:     &LiteralExpr{Value: sqltypes.NewInt64(0)},
			then:     &LiteralExpr{Value: sqltypes.NewInt64(42)},
			els:      nil,
			expected: False,
		},
	}

	for _, tt := range tests {
		expr := &IfExpr{When: tt.when, Then: tt.then, Else: tt.els}
		t.Run(expr.String(), func(t *testing.T) {
			actual, err := expr.Eval(ctx, schema.Row{}, nil)
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual)
		})
	}
}
