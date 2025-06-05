package engine

import (
	"context"
	"fmt"
	"sort"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type OrderPlan struct {
	Input     Plan
	Expr      Expr
	Direction string
}

var _ Plan = (*OrderPlan)(nil)

func (p *OrderPlan) Run(ctx context.Context, binds map[string]*querypb.BindVariable) (schema.Cursor, error) {
	input, err := p.Input.Run(ctx, binds)
	if err != nil {
		return nil, err
	}

	rows, err := schema.ReadAll(input)
	if err != nil {
		return nil, err
	}

	var pairs []struct {
		key Value
		row schema.Row
	}

	for _, row := range rows {
		val, err := p.Expr.Eval(ctx, row, binds)
		if err != nil {
			return nil, err
		}
		pairs = append(pairs, struct {
			key Value
			row schema.Row
		}{key: val, row: row})
	}

	sort.SliceStable(pairs, func(i, j int) bool {
		cmp, err := Compare(pairs[i].key, pairs[j].key)
		if err != nil {
			return false
		}
		if p.Direction == sqlparser.DescScr {
			cmp = -cmp
		}
		return cmp < 0
	})

	rows = nil
	for _, pair := range pairs {
		rows = append(rows, pair.row)
	}
	return schema.NewInMemoryCursor(rows), nil
}

func (p *OrderPlan) String() string {
	return fmt.Sprintf("OrderPlan(%s, %s, %s)", p.Input.String(), p.Expr.String(), p.Direction)
}
