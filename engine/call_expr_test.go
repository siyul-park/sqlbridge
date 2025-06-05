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

func TestCallExpr_Eval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	d := NewDispatcher(WithFunction("foo", func(args []Value) (Value, error) {
		return NewTuple(args), nil
	}))

	expr := &CallExpr{
		Dispatcher: d,
		Name:       sqlparser.NewColIdent("foo"),
		Input:      &LiteralExpr{Value: sqltypes.NewInt64(42)},
	}

	actual, err := expr.Eval(ctx, schema.Row{}, nil)
	require.NoError(t, err)
	require.Equal(t, NewTuple([]Value{NewInt64(42)}), actual)
}
