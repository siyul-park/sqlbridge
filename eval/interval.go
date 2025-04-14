package eval

import (
	"context"
	"fmt"
	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type Interval struct {
	Input Expr
	Unit  string
}

var _ Expr = (*Interval)(nil)

func (e *Interval) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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

func (e *Interval) String() string {
	return fmt.Sprintf("INTERVAL %s %s", e.Input, e.Unit)
}
