package eval

import (
	"context"
	"fmt"
	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type Exists struct {
	Input Expr
}

var _ Expr = (*Exists)(nil)

func (e *Exists) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	val, err := e.Input.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	switch v := val.(type) {
	case *Tuple:
		return NewBool(len(v.Values()) > 0), nil
	default:
		return NewBool(ToBool(v)), nil
	}
}

func (e *Exists) String() string {
	return fmt.Sprintf("Exists(%s)", e.Input.String())
}
