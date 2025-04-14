package eval

import (
	"context"
	"fmt"
	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type IsNull struct {
	Input Expr
}

var _ Expr = (*IsNull)(nil)

func (e *IsNull) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	val, err := e.Input.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	return NewBool(val == nil), nil
}

func (e *IsNull) String() string {
	return fmt.Sprintf("IsNull(%s)", e.Input.String())
}
