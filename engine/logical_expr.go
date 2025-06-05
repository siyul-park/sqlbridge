package engine

import (
	"context"
	"fmt"

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

func (e *IfExpr) String() string {
	return fmt.Sprintf("If(%s, %s, %s)", e.When.String(), e.Then.String(), e.Else.String())
}
