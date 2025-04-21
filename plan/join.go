package plan

import (
	"context"
	"fmt"
	"strings"

	"github.com/siyul-park/sqlbridge/eval"
	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type Join struct {
	Left  Plan
	Right Plan
	Kind  string
	Expr  eval.Expr
}

var _ Plan = (*Join)(nil)

func (p *Join) Run(ctx context.Context, binds map[string]*querypb.BindVariable) (schema.Cursor, error) {
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
				} else if !eval.ToBool(val) {
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

func (p *Join) String() string {
	var sb strings.Builder
	sb.WriteString("Join(")
	sb.WriteString(p.Left.String())
	sb.WriteString(", ")
	sb.WriteString(p.Right.String())
	sb.WriteString(", ")
	sb.WriteString(p.Kind)
	if p.Expr != nil {
		sb.WriteString(", ")
		sb.WriteString(p.Expr.String())
	}
	sb.WriteString(")")
	return sb.String()
}
