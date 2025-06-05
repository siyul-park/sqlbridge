package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type ParenExpr struct {
	Exprs []Expr
}

var _ Expr = (*ParenExpr)(nil)

func (e *ParenExpr) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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

func (e *ParenExpr) String() string {
	parts := make([]string, len(e.Exprs))
	for i, e := range e.Exprs {
		parts[i] = e.String()
	}
	return fmt.Sprintf("Paren(%s)", strings.Join(parts, ", "))
}

type UnpackExpr struct {
	Exprs []Expr
}

var _ Expr = (*UnpackExpr)(nil)

func (e *UnpackExpr) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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

func (e *UnpackExpr) String() string {
	parts := make([]string, len(e.Exprs))
	for i, e := range e.Exprs {
		parts[i] = e.String()
	}
	return fmt.Sprintf("Unpack(%s)", strings.Join(parts, ", "))
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

func (e *IndexExpr) String() string {
	return fmt.Sprintf("Index(%s, %s)", e.Left.String(), e.Right.String())
}
