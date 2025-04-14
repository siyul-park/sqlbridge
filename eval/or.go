package eval

import (
	"context"
	"fmt"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type Or struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*Or)(nil)

func (e *Or) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	left, err := e.Left.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	if ToBool(left) {
		return True, nil
	}

	right, err := e.Right.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	return NewBool(ToBool(right)), nil
}

func (e *Or) String() string {
	return fmt.Sprintf("Or(%s, %s)", e.Left.String(), e.Right.String())
}
