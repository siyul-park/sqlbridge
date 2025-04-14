package eval

import (
	"context"
	"fmt"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type GreaterThan struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*GreaterThan)(nil)

func (e *GreaterThan) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	left, err := e.Left.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	right, err := e.Right.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}

	cmp, err := Compare(left, right)
	if err != nil {
		return nil, err
	}
	return NewBool(cmp > 0), nil
}

func (e *GreaterThan) String() string {
	return fmt.Sprintf("GreaterThan(%s, %s)", e.Left.String(), e.Right.String())
}
