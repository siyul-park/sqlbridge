package engine

import (
	"context"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type SubqueryExpr struct {
	Input Plan
}

var _ Expr = (*SubqueryExpr)(nil)

func (e *SubqueryExpr) Eval(ctx context.Context, _ schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	input, err := e.Input.Run(ctx, binds)
	if err != nil {
		return nil, err
	}

	rows, err := schema.ReadAll(input)
	if err != nil {
		return nil, err
	}

	vals := make([]Value, 0, len(rows))
	for _, r := range rows {
		cols := make([]Value, 0, len(r.Values))
		for _, col := range r.Values {
			val, err := FromSQL(col)
			if err != nil {
				return nil, err
			}
			cols = append(cols, val)
		}

		var val Value
		if len(cols) == 1 {
			val = cols[0]
		} else if len(cols) > 1 {
			val = NewTuple(cols)
		}
		vals = append(vals, val)
	}
	return NewTuple(vals), nil
}

func (e *SubqueryExpr) Walk(f func(Expr) (bool, error)) (bool, error) {
	return f(e)
}

func (e *SubqueryExpr) Copy() Expr {
	return &SubqueryExpr{
		Input: e.Input,
	}
}

func (e *SubqueryExpr) String() string {
	return "Subquery(" + e.Input.String() + ")"
}
