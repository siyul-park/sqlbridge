package plan

import (
	"context"
	"fmt"
	"sort"

	"github.com/siyul-park/sqlbridge/eval"
	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type Order struct {
	Input     Plan
	Expr      eval.Expr
	Direction string
}

var _ Plan = (*Order)(nil)

func (p *Order) Run(ctx context.Context, binds map[string]*querypb.BindVariable) (schema.Cursor, error) {
	input, err := p.Input.Run(ctx, binds)
	if err != nil {
		return nil, err
	}

	rows, err := schema.ReadAll(input)
	if err != nil {
		return nil, err
	}

	var pairs []struct {
		row   schema.Row
		value eval.Value
	}

	for _, row := range rows {
		val, err := p.Expr.Eval(ctx, row, binds)
		if err != nil {
			return nil, err
		}
		pairs = append(pairs, struct {
			row   schema.Row
			value eval.Value
		}{row: row, value: val})
	}

	sort.SliceStable(pairs, func(i, j int) bool {
		cmp, err := eval.Compare(pairs[i].value, pairs[j].value)
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

func (p *Order) String() string {
	return fmt.Sprintf("Order(%s, %s, %s)", p.Input.String(), p.Expr.String(), p.Direction)
}
