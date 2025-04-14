package eval

import (
	"context"
	"fmt"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type BitOr struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*BitOr)(nil)

func (e *BitOr) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	left, err := e.Left.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	right, err := e.Right.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}

	switch l := left.(type) {
	case *Int64:
		r, err := ToInt(right)
		if err != nil {
			return nil, err
		}
		return NewInt64(l.Int() | r), nil
	case *Uint64:
		r, err := ToUint(right)
		if err != nil {
			return nil, err
		}
		return NewUint64(l.Uint() | r), nil
	default:
		return nil, fmt.Errorf("cannot bitwise OR %T with %T", left, right)
	}
}

func (e *BitOr) String() string {
	return fmt.Sprintf("BitOr(%s, %s)", e.Left, e.Right)
}
