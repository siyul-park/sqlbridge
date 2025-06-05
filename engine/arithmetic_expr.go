package engine

import (
	"context"
	"fmt"
	"math"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type AddExpr struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*AddExpr)(nil)

func (e *AddExpr) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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
	case *VarChar:
		r, ok := right.(*VarChar)
		if !ok {
			return nil, fmt.Errorf("cannot plus VarChar with %T", right)
		}
		return NewVarChar(l.String() + r.String()), nil
	default:
		return nil, fmt.Errorf("cannot plus %T with %T", left, right)
	}
}

func (e *AddExpr) String() string {
	return fmt.Sprintf("Add(%s, %s)", e.Left, e.Right)
}

type SubExpr struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*SubExpr)(nil)

func (e *SubExpr) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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

func (e *SubExpr) String() string {
	return fmt.Sprintf("Sub(%s, %s)", e.Left, e.Right)
}

type MulExpr struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*MulExpr)(nil)

func (e *MulExpr) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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

func (e *MulExpr) String() string {
	return e.Left.String() + " * " + e.Right.String()
}

type DivExpr struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*DivExpr)(nil)

func (e *DivExpr) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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

func (e *DivExpr) String() string {
	return fmt.Sprintf("Div(%s, %s)", e.Left, e.Right)
}

type ModExpr struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*ModExpr)(nil)

func (e *ModExpr) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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

func (e *ModExpr) String() string {
	return fmt.Sprintf("Mod(%s, %s)", e.Left, e.Right)
}

type ShiftLeftExpr struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*ShiftLeftExpr)(nil)

func (e *ShiftLeftExpr) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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
		if r < 0 {
			return nil, fmt.Errorf("cannot shift left by negative value %d", r)
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

func (e *ShiftLeftExpr) String() string {
	return fmt.Sprintf("ShiftLeft(%s, %s)", e.Left, e.Right)
}

type ShiftRightExpr struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*ShiftRightExpr)(nil)

func (e *ShiftRightExpr) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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
		if r < 0 {
			return nil, fmt.Errorf("cannot shift right by negative value %d", r)
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

func (e *ShiftRightExpr) String() string {
	return fmt.Sprintf("ShiftRight(%s, %s)", e.Left, e.Right)
}

type BitNotExpr struct {
	Input Expr
}

var _ Expr = (*BitNotExpr)(nil)

func (e *BitNotExpr) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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

func (e *BitNotExpr) String() string {
	return fmt.Sprintf("BitNot(%s)", e.Input)
}
