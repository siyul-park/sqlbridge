package eval

import (
	"context"
	"fmt"
	"sort"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type Distinct struct {
	Input Expr
}

var _ Expr = (*Distinct)(nil)

func (e *Distinct) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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

func (e *Distinct) String() string {
	return fmt.Sprintf("Distinct(%s)", e.Input.String())
}

type Order struct {
	Left      Expr
	Right     Expr
	Direction string
}

var _ Expr = (*Order)(nil)

func (e *Order) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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

func (e *Order) String() string {
	return fmt.Sprintf("Order(%s, %s)", e.Left.String(), e.Right.String())
}
