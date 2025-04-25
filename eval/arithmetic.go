package eval

import (
	"context"
	"fmt"
	"math"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type Add struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*Add)(nil)

func (e *Add) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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
			return nil, fmt.Errorf("cannot plus Int64 with %T", right)
		}
		return NewInt64(l.Int() + r.Int()), nil
	case *Uint64:
		r, ok := right.(*Uint64)
		if !ok {
			return nil, fmt.Errorf("cannot plus Uint64 with %T", right)
		}
		return NewUint64(l.Uint() + r.Uint()), nil
	case *Float64:
		r, ok := right.(*Float64)
		if !ok {
			return nil, fmt.Errorf("cannot plus Float64 with %T", right)
		}
		return NewFloat64(l.Float() + r.Float()), nil
	case *String:
		r, ok := right.(*String)
		if !ok {
			return nil, fmt.Errorf("cannot plus String with %T", right)
		}
		return NewString(l.String() + r.String()), nil
	default:
		return nil, fmt.Errorf("cannot plus %T with %T", left, right)
	}
}

func (e *Add) String() string {
	return fmt.Sprintf("Add(%s, %s)", e.Left, e.Right)
}

type Sub struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*Sub)(nil)

func (e *Sub) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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

func (e *Sub) String() string {
	return fmt.Sprintf("Sub(%s, %s)", e.Left, e.Right)
}

type Mul struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*Mul)(nil)

func (e *Mul) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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

func (e *Mul) String() string {
	return e.Left.String() + " * " + e.Right.String()
}

type Div struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*Div)(nil)

func (e *Div) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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

func (e *Div) String() string {
	return fmt.Sprintf("Div(%s, %s)", e.Left, e.Right)
}

type Mod struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*Mod)(nil)

func (e *Mod) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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

func (e *Mod) String() string {
	return fmt.Sprintf("Mod(%s, %s)", e.Left, e.Right)
}

type ShiftLeft struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*ShiftLeft)(nil)

func (e *ShiftLeft) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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
		return NewInt64(l.Int() << r), nil
	case *Uint64:
		r, err := ToUint(right)
		if err != nil {
			return nil, err
		}
		return NewUint64(l.Uint() << r), nil
	default:
		return nil, fmt.Errorf("cannot shift %T by %T", left, right)
	}
}

func (e *ShiftLeft) String() string {
	return fmt.Sprintf("ShiftLeft(%s, %s)", e.Left, e.Right)
}

type ShiftRight struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*ShiftRight)(nil)

func (e *ShiftRight) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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
		return NewInt64(l.Int() >> r), nil
	case *Uint64:
		r, err := ToUint(right)
		if err != nil {
			return nil, err
		}
		return NewUint64(l.Uint() >> r), nil
	default:
		return nil, fmt.Errorf("cannot shift %T by %T", left, right)
	}
}

func (e *ShiftRight) String() string {
	return fmt.Sprintf("ShiftRight(%s, %s)", e.Left, e.Right)
}

type BitNot struct {
	Input Expr
}

var _ Expr = (*BitNot)(nil)

func (e *BitNot) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	input, err := e.Input.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}

	switch l := input.(type) {
	case *Int64:
		return NewInt64(^l.Int()), nil
	case *Uint64:
		return NewUint64(^l.Uint()), nil
	default:
		return nil, fmt.Errorf("cannot apply bitwise NOT to %T", input)
	}
}

func (e *BitNot) String() string {
	return fmt.Sprintf("BitNot(%s)", e.Input)
}
