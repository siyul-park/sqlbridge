package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type JoinPlan struct {
	Left  Plan
	Right Plan
	Kind  string
	Expr  Expr
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
	switch p.Kind {
	case sqlparser.JoinStr:
		for _, l := range lhs {
			for _, r := range rhs {
				join := schema.Row{
					Columns: append(l.Columns, r.Columns...),
					Values:  append(l.Values, r.Values...),
				}
				if val, err := p.Expr.Eval(ctx, join, binds); err != nil {
					return nil, err
				} else if !ToBool(val) {
					continue
				}
				joins = append(joins, join)
			}
		}
	default:
		return nil, fmt.Errorf("unknown join type: %s", p.Kind)
	}

	return schema.NewInMemoryCursor(joins), nil
}

func (p *JoinPlan) String() string {
	var b strings.Builder
	b.WriteString("JoinPlan(")
	b.WriteString(p.Left.String())
	b.WriteString(", ")
	b.WriteString(p.Right.String())
	b.WriteString(", ")
	b.WriteString(p.Kind)
	if p.Expr != nil {
		b.WriteString(", ")
		b.WriteString(p.Expr.String())
	}
	b.WriteString(")")
	return b.String()
}
