package eval

import (
	"context"
	"fmt"
	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/querypb"
	"sort"
)

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
		left  Value
		right Value
	}

	var pairs []pair
	for _, r := range row.Children {
		lv, err := e.Left.Eval(ctx, r, binds)
		if err != nil {
			return nil, err
		}
		rv, err := e.Right.Eval(ctx, r, binds)
		if err != nil {
			return nil, err
		}
		pairs = append(pairs, pair{left: lv, right: rv})
	}

	sort.SliceStable(pairs, func(i, j int) bool {
		cmp, err := Compare(pairs[i].right, pairs[j].right)
		if err != nil {
			return false
		}
		if e.Direction == sqlparser.DescScr {
			cmp = -cmp
		}
		return cmp < 0
	})

	var values []Value
	for _, p := range pairs {
		values = append(values, p.left)
	}
	return NewTuple(values), nil
}

func (e *Order) String() string {
	return fmt.Sprintf("Order(%s, %s)", e.Left.String(), e.Right.String())
}
