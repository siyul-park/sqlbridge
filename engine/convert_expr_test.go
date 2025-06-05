package engine

import (
	"context"
	"testing"
	"time"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/stretchr/testify/require"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
)

func TestConvertExpr_Eval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	tests := []struct {
		input    Expr
		typ      *sqlparser.ConvertType
		expected Value
	}{
		{
			input:    &LiteralExpr{Value: sqltypes.NewVarChar("123")},
			typ:      &sqlparser.ConvertType{Type: "INT64"},
			expected: NewInt64(123),
		},
		{
			input:    &LiteralExpr{Value: sqltypes.NewVarChar("123.45")},
			typ:      &sqlparser.ConvertType{Type: "FLOAT64"},
			expected: NewFloat64(123.45),
		},
		{
			input:    &LiteralExpr{Value: sqltypes.NewInt64(456)},
			typ:      &sqlparser.ConvertType{Type: "VARCHAR"},
			expected: NewVarChar("456"),
		},
		{
			input:    &LiteralExpr{Value: sqltypes.NewVarChar("true")},
			typ:      &sqlparser.ConvertType{Type: "VARBINARY"},
			expected: NewVarBinary([]byte("true")),
		},
	}

	for _, tt := range tests {
		expr := &ConvertExpr{
			Input: tt.input,
			Type:  tt.typ,
		}
		t.Run(expr.String(), func(t *testing.T) {
			actual, err := expr.Eval(ctx, schema.Row{}, nil)
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual)
		})
	}
}
