package eval

import (
	"context"
	"fmt"
	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type Divide struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*Divide)(nil)

func (e *Divide) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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
			return nil, fmt.Errorf("cannot divide Int64 by %T", right)
		}
		if r.Int() == 0 {
			return nil, fmt.Errorf("division by zero")
		}
		return NewInt64(l.Int() / r.Int()), nil
	case *Uint64:
		r, ok := right.(*Uint64)
		if !ok {
			return nil, fmt.Errorf("cannot divide Uint64 by %T", right)
		}
		if r.Uint() == 0 {
			return nil, fmt.Errorf("division by zero")
		}
		return NewUint64(l.Uint() / r.Uint()), nil
	case *Float64:
		r, ok := right.(*Float64)
		if !ok {
			return nil, fmt.Errorf("cannot divide Float64 by %T", right)
		}
		if r.Float() == 0 {
			return nil, fmt.Errorf("division by zero")
		}
		return NewFloat64(l.Float() / r.Float()), nil
	default:
		return nil, fmt.Errorf("cannot divide %T by %T", left, right)
	}
}

func (e *Divide) String() string {
	return fmt.Sprintf("Divide(%s, %s)", e.Left, e.Right)
}
