package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type AndExpr struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*AndExpr)(nil)

func (e *AndExpr) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	left, err := e.Left.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	if !ToBool(left) {
		return False, nil
	}

	right, err := e.Right.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	return NewBool(ToBool(right)), nil
}

func (e *AndExpr) Walk(f func(Expr) (bool, error)) (bool, error) {
	if cont, err := f(e); !cont || err != nil {
		return cont, err
	}
	if cont, err := e.Left.Walk(f); !cont || err != nil {
		return cont, err
	}
	return e.Right.Walk(f)
}

func (e *AndExpr) Copy() Expr {
	return &AndExpr{
		Left:  e.Left.Copy(),
		Right: e.Right.Copy(),
	}
}

func (e *AndExpr) String() string {
	return fmt.Sprintf("And(%s, %s)", e.Left.String(), e.Right.String())
}

type OrExpr struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*OrExpr)(nil)

func (e *OrExpr) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	left, err := e.Left.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	if ToBool(left) {
		return True, nil
	}

	right, err := e.Right.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	return NewBool(ToBool(right)), nil
}

func (e *OrExpr) Walk(f func(Expr) (bool, error)) (bool, error) {
	if cont, err := f(e); !cont || err != nil {
		return cont, err
	}
	if cont, err := e.Left.Walk(f); !cont || err != nil {
		return cont, err
	}
	return e.Right.Walk(f)
}

func (e *OrExpr) Copy() Expr {
	return &OrExpr{
		Left:  e.Left.Copy(),
		Right: e.Right.Copy(),
	}
}

func (e *OrExpr) String() string {
	return fmt.Sprintf("Or(%s, %s)", e.Left.String(), e.Right.String())
}

type NotExpr struct {
	Input Expr
}

var _ Expr = (*NotExpr)(nil)

func (e *NotExpr) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	val, err := e.Input.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	return NewBool(!ToBool(val)), nil
}

func (e *NotExpr) Walk(f func(Expr) (bool, error)) (bool, error) {
	if cont, err := f(e); !cont || err != nil {
		return cont, err
	}
	return e.Input.Walk(f)
}

func (e *NotExpr) Copy() Expr {
	return &NotExpr{
		Input: e.Input.Copy(),
	}
}

func (e *NotExpr) String() string {
	return fmt.Sprintf("Not(%s)", e.Input.String())
}

type IfExpr struct {
	When Expr
	Then Expr
	Else Expr
}

var _ Expr = (*IfExpr)(nil)

func (e *IfExpr) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	cond, err := e.When.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	if ToBool(cond) {
		return e.Then.Eval(ctx, row, binds)
	}
	if e.Else != nil {
		return e.Else.Eval(ctx, row, binds)
	}
	return False, nil
}

func (e *IfExpr) Walk(f func(Expr) (bool, error)) (bool, error) {
	if cont, err := f(e); !cont || err != nil {
		return cont, err
	}
	if cont, err := e.When.Walk(f); !cont || err != nil {
		return cont, err
	}
	if cont, err := e.Then.Walk(f); !cont || err != nil {
		return cont, err
	}
	if e.Else != nil {
		return e.Else.Walk(f)
	}
	return true, nil
}

func (e *IfExpr) Copy() Expr {
	expr := &IfExpr{
		When: e.When.Copy(),
		Then: e.Then.Copy(),
	}
	if e.Else != nil {
		expr.Else = e.Else.Copy()
	}
	return expr
}

func (e *IfExpr) String() string {
	var b strings.Builder
	b.WriteString("If(")
	b.WriteString(e.When.String())
	b.WriteString(", ")
	b.WriteString(e.Then.String())
	if e.Else != nil {
		b.WriteString(", ")
		b.WriteString(e.Else.String())
	}
	b.WriteString(")")
	return b.String()
}
