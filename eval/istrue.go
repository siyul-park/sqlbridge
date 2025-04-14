package eval

import (
	"context"
	"fmt"
	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type IsTrue struct {
	Input Expr
}

var _ Expr = (*IsTrue)(nil)

func (e *IsTrue) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	val, err := e.Input.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	return NewBool(ToBool(val)), nil
}

func (e *IsTrue) String() string {
	return fmt.Sprintf("IsTrue(%s)", e.Input.String())
}
