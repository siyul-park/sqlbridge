package engine

import (
	"context"
	"testing"
	"time"

	"github.com/siyul-park/sqlbridge/schema"

	"github.com/stretchr/testify/require"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
)

func TestLiteralExpr_Eval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	tests := []struct {
		value    sqltypes.Value
		expected Value
	}{
		{
			value:    sqltypes.NewInt64(42),
			expected: NewInt64(42),
		},
		{
			value:    sqltypes.NewVarChar("tt"),
			expected: NewVarChar("tt"),
		},
		{
			value:    sqltypes.NewFloat64(3.14),
			expected: NewFloat64(3.14),
		},
		{
			value:    sqltypes.NULL,
			expected: nil,
		},
	}

	for _, tt := range tests {
		expr := &LiteralExpr{Value: tt.value}
		t.Run(expr.String(), func(t *testing.T) {
			result, err := expr.Eval(ctx, schema.Row{}, nil)
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}
