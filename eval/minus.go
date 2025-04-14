package eval

import (
	"context"
	"fmt"
	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type Minus struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*Minus)(nil)

func (e *Minus) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	left, err := e.Left.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	right, err := e.Right.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}

	left, right, err = Promote(left, right)
	if err != nil {
		return nil, err
	}

	switch l := left.(type) {
	case *Int64:
		r, ok := right.(*Int64)
		if !ok {
			return nil, fmt.Errorf("cannot minus Int64 with %T", right)
		}
		return NewInt64(l.Int() - r.Int()), nil
	case *Uint64:
		r, ok := right.(*Uint64)
		if !ok {
			return nil, fmt.Errorf("cannot minus Uint64 with %T", right)
		}
		return NewUint64(l.Uint() - r.Uint()), nil
	case *Float64:
		r, ok := right.(*Float64)
		if !ok {
			return nil, fmt.Errorf("cannot minus Float64 with %T", right)
		}
		return NewFloat64(l.Float() - r.Float()), nil
	default:
		return nil, fmt.Errorf("cannot minus %T with %T", left, right)
	}
}

func (e *Minus) String() string {
	return fmt.Sprintf("Minus(%s, %s)", e.Left, e.Right)
}
