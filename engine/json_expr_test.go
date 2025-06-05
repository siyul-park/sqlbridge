package engine

import (
	"context"
	"testing"
	"time"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/stretchr/testify/require"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
)

func TestJSONExtractExpr_Eval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	tests := []struct {
		left     Expr
		right    Expr
		expected Value
	}{
		{
			left:     &LiteralExpr{Value: sqltypes.NewVarChar(`{"foo": 100}`)},
			right:    &LiteralExpr{Value: sqltypes.NewVarChar("$.foo")},
			expected: NewValue(float64(100)),
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewVarChar(`{"nested": {"key": "val"}}`)},
			right:    &LiteralExpr{Value: sqltypes.NewVarChar("$.nested.key")},
			expected: NewValue("val"),
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewVarChar(`{"arr": [1, 2, 3]}`)},
			right:    &LiteralExpr{Value: sqltypes.NewVarChar("$.arr[2]")},
			expected: NewValue(float64(3)),
		},
		{
			left:     &LiteralExpr{Value: sqltypes.NewVarChar(`{"arr": [{"x": 1}, {"x": 2}]}`)},
			right:    &LiteralExpr{Value: sqltypes.NewVarChar("$.arr[1].x")},
			expected: NewValue(float64(2)),
		},
	}

	for _, tt := range tests {
		expr := &JSONExtractExpr{
			Left:  tt.left,
			Right: tt.right,
		}
		t.Run(expr.String(), func(t *testing.T) {
			actual, err := expr.Eval(ctx, schema.Row{}, nil)
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual)
		})
	}
}
