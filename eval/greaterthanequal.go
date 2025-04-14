package eval

import (
	"context"
	"fmt"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type GreaterThanEqual struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*GreaterThanEqual)(nil)

func (e *GreaterThanEqual) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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
	return NewBool(cmp >= 0), nil
}

func (e *GreaterThanEqual) String() string {
	return fmt.Sprintf("GreaterThanEqual(%s, %s)", e.Left.String(), e.Right.String())
}
