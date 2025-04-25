package eval

import (
	"context"
	"fmt"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type And struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*And)(nil)

func (e *And) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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

func (e *And) String() string {
	return fmt.Sprintf("And(%s, %s)", e.Left.String(), e.Right.String())
}

type Or struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*Or)(nil)

func (e *Or) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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

func (e *Or) String() string {
	return fmt.Sprintf("Or(%s, %s)", e.Left.String(), e.Right.String())
}

type Not struct {
	Input Expr
}

var _ Expr = (*Not)(nil)

func (e *Not) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	val, err := e.Input.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	return NewBool(!ToBool(val)), nil
}

func (e *Not) String() string {
	return fmt.Sprintf("Not(%s)", e.Input.String())
}

type If struct {
	When Expr
	Then Expr
	Else Expr
}

var _ Expr = (*If)(nil)

func (e *If) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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

func (e *If) String() string {
	return fmt.Sprintf("If(%s, %s, %s)", e.When.String(), e.Then.String(), e.Else.String())
}
