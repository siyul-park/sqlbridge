package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type TupleExpr struct {
	Exprs []Expr
}

var _ Expr = (*TupleExpr)(nil)

func (e *TupleExpr) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	var vals []Value
	for _, elem := range e.Exprs {
		val, err := elem.Eval(ctx, row, binds)
		if err != nil {
			return nil, err
		}
		vals = append(vals, val)
	}
	if len(vals) == 1 {
		return vals[0], nil
	}
	return NewTuple(vals), nil
}

func (e *TupleExpr) Walk(f func(Expr) (bool, error)) (bool, error) {
	if cont, err := f(e); !cont || err != nil {
		return cont, err
	}
	for _, expr := range e.Exprs {
		if cont, err := expr.Walk(f); !cont || err != nil {
			return cont, err
		}
	}
	return true, nil
}

func (e *TupleExpr) Copy() Expr {
	exprs := make([]Expr, len(e.Exprs))
	for i, expr := range e.Exprs {
		exprs[i] = expr.Copy()
	}
	return &TupleExpr{
		Exprs: exprs,
	}
}

func (e *TupleExpr) String() string {
	parts := make([]string, len(e.Exprs))
	for i, e := range e.Exprs {
		parts[i] = e.String()
	}
	return fmt.Sprintf("Tuple(%s)", strings.Join(parts, ", "))
}

type SpreadExpr struct {
	Exprs []Expr
}

var _ Expr = (*SpreadExpr)(nil)

func (e *SpreadExpr) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	var vals []Value
	for _, elem := range e.Exprs {
		val, err := elem.Eval(ctx, row, binds)
		if err != nil {
			return nil, err
		}
		switch val := val.(type) {
		case *Tuple:
			vals = append(vals, val.Values()...)
		default:
			vals = append(vals, val)
		}
	}
	if len(vals) == 1 {
		return vals[0], nil
	}
	return NewTuple(vals), nil
}

func (e *SpreadExpr) Walk(f func(Expr) (bool, error)) (bool, error) {
	if cont, err := f(e); !cont || err != nil {
		return cont, err
	}
	for _, expr := range e.Exprs {
		if cont, err := expr.Walk(f); !cont || err != nil {
			return cont, err
		}
	}
	return true, nil
}

func (e *SpreadExpr) Copy() Expr {
	exprs := make([]Expr, len(e.Exprs))
	for i, expr := range e.Exprs {
		exprs[i] = expr.Copy()
	}
	return &SpreadExpr{
		Exprs: exprs,
	}
}

func (e *SpreadExpr) String() string {
	parts := make([]string, len(e.Exprs))
	for i, e := range e.Exprs {
		parts[i] = e.String()
	}
	return fmt.Sprintf("Spread(%s)", strings.Join(parts, ", "))
}

type IndexExpr struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*IndexExpr)(nil)

func (e *IndexExpr) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	left, err := e.Left.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	right, err := e.Right.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}

	switch lhs := left.(type) {
	case *Tuple:
		values := lhs.Values()
		index, err := ToInt(right)
		if err != nil {
			return nil, err
		}
		if int(index) >= len(values) || int(index) < 0 {
			return nil, nil
		}
		return values[int(index)], nil
	default:
		return lhs, nil
	}
}

func (e *IndexExpr) Walk(f func(Expr) (bool, error)) (bool, error) {
	if cont, err := f(e); !cont || err != nil {
		return cont, err
	}
	if cont, err := e.Left.Walk(f); !cont || err != nil {
		return cont, err
	}
	return e.Right.Walk(f)
}

func (e *IndexExpr) Copy() Expr {
	return &IndexExpr{
		Left:  e.Left.Copy(),
		Right: e.Right.Copy(),
	}
}

func (e *IndexExpr) String() string {
	return fmt.Sprintf("Indexes(%s, %s)", e.Left.String(), e.Right.String())
}
