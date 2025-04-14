package plan

import (
	"context"
	"fmt"

	"github.com/siyul-park/sqlbridge/eval"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type Join struct {
	Left  Plan
	Right Plan
	Join  string
	On    eval.Expr
	Using []eval.Expr
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
	switch p.Join {
	case sqlparser.JoinStr:
		for _, l := range lhs {
			for _, r := range rhs {
				join := schema.Row{
					Columns: append(l.Columns, r.Columns...),
					Values:  append(l.Values, r.Values...),
				}

				if p.On != nil {
					val, err := p.On.Eval(ctx, join, binds)
					if err != nil {
						return nil, err
					}
					if !eval.ToBool(val) {
						continue
					}
				}

				ok := true
				for _, using := range p.Using {
					lv, err := using.Eval(ctx, l, binds)
					if err != nil {
						return nil, err
					}
					rv, err := using.Eval(ctx, r, binds)
					if err != nil {
						return nil, err
					}

					if cmp, err := eval.Compare(lv, rv); err != nil {
						return nil, err
					} else if cmp == 0 {
						ok = false
						break
					}
				}
				if !ok {
					continue
				}

				joins = append(joins, join)
			}
		}
	default:
		return nil, fmt.Errorf("unknown join type: %s", p.Join)
	}

	return schema.NewInMemoryCursor(joins), nil
}

func (p *Join) String() string {
	return fmt.Sprintf("Join(%s, %s, %s, %s)", p.Left.String(), p.Right.String(), p.Join, p.On.String())
}
