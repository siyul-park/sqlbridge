package eval

import (
	"context"
	"fmt"
	"math"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type Modulo struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*Modulo)(nil)

func (e *Modulo) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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
			return nil, fmt.Errorf("cannot mod Int64 with %T", right)
		}
		return NewInt64(l.Int() % r.Int()), nil
	case *Uint64:
		r, ok := right.(*Uint64)
		if !ok {
			return nil, fmt.Errorf("cannot mod Uint64 with %T", right)
		}
		return NewUint64(l.Uint() % r.Uint()), nil
	case *Float64:
		r, ok := right.(*Float64)
		if !ok {
			return nil, fmt.Errorf("cannot mod Float64 with %T", right)
		}
		return NewFloat64(math.Mod(l.Float(), r.Float())), nil
	default:
		return nil, fmt.Errorf("cannot mod %T with %T", left, right)
	}
}

func (e *Modulo) String() string {
	return fmt.Sprintf("Modulo(%s, %s)", e.Left, e.Right)
}
