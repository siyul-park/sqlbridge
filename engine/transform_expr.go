package engine

import (
	"context"
	"fmt"
	"sort"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type DistinctExpr struct {
	Input Expr
}

var _ Expr = (*DistinctExpr)(nil)

func (e *DistinctExpr) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	input, err := e.Input.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	val, ok := input.(*Tuple)
	if !ok {
		return val, nil
	}

	var vals []Value
	for _, val := range val.Values() {
		duplicate := true
		for _, v := range vals {
			cmp, err := Compare(v, val)
			if cmp == 0 && err == nil {
				duplicate = false
				break
			}
		}
		if !duplicate {
			continue
		}
		vals = append(vals, val)
	}
	return NewTuple(vals), nil
}

func (e *DistinctExpr) Walk(f func(Expr) (bool, error)) (bool, error) {
	if cont, err := f(e); !cont || err != nil {
		return cont, err
	}
	return e.Input.Walk(f)
}

func (e *DistinctExpr) Copy() Expr {
	return &DistinctExpr{
		Input: e.Input.Copy(),
	}
}

func (e *DistinctExpr) String() string {
	return fmt.Sprintf("Distinct(%s)", e.Input.String())
}

type OrderExpr struct {
	Left      Expr
	Right     Expr
	Direction string
}

var _ Expr = (*OrderExpr)(nil)

func (e *OrderExpr) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	if len(row.Children) == 0 {
		return e.Left.Eval(ctx, row, binds)
	}

	type pair struct {
		key Value
		val schema.Row
	}

	var pairs []pair
	for _, row := range row.Children {
		key, err := e.Right.Eval(ctx, row, binds)
		if err != nil {
			return nil, err
		}
		pairs = append(pairs, pair{key: key, val: row})
	}

	sort.SliceStable(pairs, func(i, j int) bool {
		cmp, err := Compare(pairs[i].key, pairs[j].key)
		if err != nil {
			return false
		}
		if e.Direction == sqlparser.DescScr {
			cmp = -cmp
		}
		return cmp < 0
	})

	row.Children = nil
	for _, pair := range pairs {
		row.Children = append(row.Children, pair.val)
	}
	return e.Left.Eval(ctx, row, binds)
}

func (e *OrderExpr) Walk(f func(Expr) (bool, error)) (bool, error) {
	if cont, err := f(e); !cont || err != nil {
		return cont, err
	}
	if cont, err := e.Left.Walk(f); !cont || err != nil {
		return cont, err
	}
	return e.Right.Walk(f)
}

func (e *OrderExpr) Copy() Expr {
	return &OrderExpr{
		Left:      e.Left.Copy(),
		Right:     e.Right.Copy(),
		Direction: e.Direction,
	}
}

func (e *OrderExpr) String() string {
	return fmt.Sprintf("Order(%s, %s)", e.Left.String(), e.Right.String())
}
