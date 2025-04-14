package eval

import (
	"context"
	"fmt"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type LessThanEqual struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*LessThanEqual)(nil)

func (e *LessThanEqual) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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
	return NewBool(cmp <= 0), nil
}

func (e *LessThanEqual) String() string {
	return fmt.Sprintf("LessThanEqual(%s, %s)", e.Left.String(), e.Right.String())
}
