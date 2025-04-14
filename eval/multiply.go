package eval

import (
	"context"
	"fmt"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type Multiply struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*Multiply)(nil)

func (e *Multiply) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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
			return nil, fmt.Errorf("cannot multiply Int64 with %T", right)
		}
		return NewInt64(l.Int() * r.Int()), nil
	case *Uint64:
		r, ok := right.(*Uint64)
		if !ok {
			return nil, fmt.Errorf("cannot multiply Uint64 with %T", right)
		}
		return NewUint64(l.Uint() * r.Uint()), nil
	case *Float64:
		r, ok := right.(*Float64)
		if !ok {
			return nil, fmt.Errorf("cannot multiply Float64 with %T", right)
		}
		return NewFloat64(l.Float() * r.Float()), nil
	default:
		return nil, fmt.Errorf("cannot multiply %T with %T", left, right)
	}
}

func (e *Multiply) String() string {
	return e.Left.String() + " * " + e.Right.String()
}
