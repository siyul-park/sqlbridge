package engine

import (
	"context"
	"strings"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type JoinPlan struct {
	Left  Plan
	Right Plan
}

var _ Plan = (*JoinPlan)(nil)

func (p *JoinPlan) Run(ctx context.Context, binds map[string]*querypb.BindVariable) (schema.Cursor, error) {
	left, err := p.Left.Run(ctx, binds)
	if err != nil {
		return nil, err
	}
	right, err := p.Right.Run(ctx, binds)
	if err != nil {
		return nil, err
	}

	lhs, err := schema.ReadAll(left)
	if err != nil {
		return nil, err
	}
	rhs, err := schema.ReadAll(right)
	if err != nil {
		return nil, err
	}

	var joins []schema.Row
	for _, l := range lhs {
		for _, r := range rhs {
			joins = append(joins, schema.Row{
				Columns: append(l.Columns, r.Columns...),
				Values:  append(l.Values, r.Values...),
			})
		}
	}

	return schema.NewInMemoryCursor(joins), nil
}

func (p *JoinPlan) Walk(f func(Plan) (bool, error)) (bool, error) {
	if cont, err := f(p); !cont || err != nil {
		return cont, err
	}
	if cont, err := p.Left.Walk(f); !cont || err != nil {
		return cont, err
	}
	return p.Right.Walk(f)
}

func (p *JoinPlan) String() string {
	var b strings.Builder
	b.WriteString("JoinPlan(")
	b.WriteString(p.Left.String())
	b.WriteString(", ")
	b.WriteString(p.Right.String())
	b.WriteString(")")
	return b.String()
}
