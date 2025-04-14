package plan

import (
	"context"
	"fmt"

	"github.com/siyul-park/sqlbridge/eval"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type Filter struct {
	Input Plan
	Expr  eval.Expr
}

var _ Plan = (*Filter)(nil)

func (p *Filter) Run(ctx context.Context, binds map[string]*querypb.BindVariable) (schema.Cursor, error) {
	input, err := p.Input.Run(ctx, binds)
	if err != nil {
		return nil, err
	}
	return schema.NewMappedCursor(input, func(row schema.Row) (schema.Row, error) {
		val, err := p.Expr.Eval(ctx, row, binds)
		if err != nil {
			return schema.Row{}, err
		}
		if !eval.ToBool(val) {
			return schema.Row{}, nil
		}
		return row, nil
	}), nil
}

func (p *Filter) String() string {
	return fmt.Sprintf("Filter(%s, %s)", p.Input.String(), p.Expr)
}
