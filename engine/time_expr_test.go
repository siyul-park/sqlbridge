package engine

import (
	"context"
	"testing"
	"time"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/stretchr/testify/require"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
)

func TestIntervalExpr_Eval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	tests := []struct {
		input    Expr
		unit     string
		expected Value
	}{
		{
			input:    &LiteralExpr{Value: sqltypes.NewInt64(5)},
			unit:     "years",
			expected: NewInterval(5, "years"),
		},
		{
			input:    &LiteralExpr{Value: sqltypes.NewInt64(5)},
			unit:     "months",
			expected: NewInterval(5, "months"),
		},
		{
			input:    &LiteralExpr{Value: sqltypes.NewInt64(5)},
			unit:     "days",
			expected: NewInterval(5, "days"),
		},
		{
			input:    &LiteralExpr{Value: sqltypes.NewInt64(5)},
			unit:     "hours",
			expected: NewInterval(5, "hours"),
		},
		{
			input:    &LiteralExpr{Value: sqltypes.NewInt64(5)},
			unit:     "minutes",
			expected: NewInterval(5, "minutes"),
		},
		{
			input:    &LiteralExpr{Value: sqltypes.NewInt64(5)},
			unit:     "seconds",
			expected: NewInterval(5, "seconds"),
		},
	}

	for _, tt := range tests {
		expr := &IntervalExpr{Input: tt.input, Unit: tt.unit}
		t.Run(expr.String(), func(t *testing.T) {
			actual, err := expr.Eval(ctx, schema.Row{}, nil)
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual)
		})
	}
	defer cancel()

}
