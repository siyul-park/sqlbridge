package plan

import (
	"context"
	"fmt"
	"strings"

	"github.com/siyul-park/sqlbridge/eval"
	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/querypb"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
)

type Group struct {
	Input Plan
	Exprs []eval.Expr
}

var _ Plan = (*Group)(nil)

func (p *Group) Run(ctx context.Context, binds map[string]*querypb.BindVariable) (schema.Cursor, error) {
	input, err := p.Input.Run(ctx, binds)
	if err != nil {
		return nil, err
	}

	rows, err := schema.ReadAll(input)
	if err != nil {
		return nil, err
	}

	var keys []*eval.Tuple
	var children [][]schema.Row
	for _, row := range rows {
		var vals []eval.Value
		for _, expr := range p.Exprs {
			val, err := expr.Eval(ctx, row, binds)
			if err != nil {
				return nil, err
			}
			vals = append(vals, val)
		}
		key := eval.NewTuple(vals)

		ok := false
		for i, k := range keys {
			cmp, err := eval.Compare(k, key)
			if cmp == 0 && err == nil {
				children[i] = append(children[i], row)
				ok = true
				break
			}
		}
		if !ok {
			keys = append(keys, key)
			children = append(children, []schema.Row{row})
		}
	}

	var group []schema.Row
	for _, rows := range children {
		columns := make([]*sqlparser.ColName, len(rows[0].Columns))
		copy(columns, rows[0].Columns)

		values := make([]sqltypes.Value, len(rows[0].Values))
		copy(values, rows[0].Values)

		for _, row := range rows[1:] {
			for i := 0; i < len(columns); i++ {
				val, _ := row.Get(columns[i])

				v1, err := eval.FromSQL(values[i])
				if err != nil {
					return nil, err
				}
				v2, err := eval.FromSQL(val)
				if err != nil {
					return nil, err
				}

				cmp, err := eval.Compare(v1, v2)
				if cmp != 0 || err != nil {
					columns = append(columns[:i], columns[i+1:]...)
					values = append(values[:i], values[i+1:]...)
					i--
				}
			}
		}

		group = append(group, schema.Row{
			Columns:  columns,
			Values:   values,
			Children: rows,
		})
	}
	return schema.NewInMemoryCursor(group), nil
}

func (p *Group) String() string {
	var exprs []string
	for _, expr := range p.Exprs {
		exprs = append(exprs, expr.String())
	}
	return fmt.Sprintf("Group(%s)", strings.Join(exprs, ", "))
}
