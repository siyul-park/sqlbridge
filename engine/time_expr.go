package engine

import (
	"context"
	"fmt"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type IntervalExpr struct {
	Input Expr
	Unit  string
}

var _ Expr = (*IntervalExpr)(nil)

func (e *IntervalExpr) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	input, err := e.Input.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	amount, err := ToInt(input)
	if err != nil {
		return nil, err
	}
	return NewDuration(amount, e.Unit), nil
}

func (e *IntervalExpr) String() string {
	return fmt.Sprintf("Interval(%s, %s)", e.Input, e.Unit)
}
