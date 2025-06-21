package engine

import (
	"context"
	"fmt"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type FilterPlan struct {
	Input Plan
	Expr  Expr
}

var _ Plan = (*FilterPlan)(nil)

func (p *FilterPlan) Run(ctx context.Context, binds map[string]*querypb.BindVariable) (schema.Cursor, error) {
	input, err := p.Input.Run(ctx, binds)
	if err != nil {
		return nil, err
	}
	return schema.NewMappedCursor(input, func(row schema.Row) (schema.Row, error) {
		val, err := p.Expr.Eval(ctx, row, binds)
		if err != nil {
			return schema.Row{}, err
		}
		if !ToBool(val) {
			return schema.Row{}, nil
		}
		return row, nil
	}), nil
}

func (p *FilterPlan) Walk(f func(Plan) (bool, error)) (bool, error) {
	if cont, err := f(p); !cont || err != nil {
		return cont, err
	}
	return p.Input.Walk(f)
}

func (p *FilterPlan) String() string {
	return fmt.Sprintf("FilterPlan(%s, %s)", p.Input.String(), p.Expr)
}
