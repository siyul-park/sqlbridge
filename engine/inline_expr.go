package engine

import (
	"context"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type InlineExpr struct {
	eval func(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error)
}

var _ Expr = (*InlineExpr)(nil)

func Eval(eval func(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error)) *InlineExpr {
	return &InlineExpr{eval: eval}
}

func (e *InlineExpr) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	return e.eval(ctx, row, binds)
}

func (e *InlineExpr) Walk(f func(Expr) (bool, error)) (bool, error) {
	return f(e)
}

func (e *InlineExpr) Copy() Expr {
	return &InlineExpr{
		eval: e.eval,
	}
}

func (e *InlineExpr) String() string {
	return "Inline(<native>)"
}
