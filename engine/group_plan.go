package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/querypb"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
)

type GroupPlan struct {
	Input Plan
	Exprs []Expr
}

var _ Plan = (*GroupPlan)(nil)

func (p *GroupPlan) Run(ctx context.Context, binds map[string]*querypb.BindVariable) (schema.Cursor, error) {
	input, err := p.Input.Run(ctx, binds)
	if err != nil {
		return nil, err
	}

	rows, err := schema.ReadAll(input)
	if err != nil {
		return nil, err
	}

	var keys []*Tuple
	var children [][]schema.Row
	for _, row := range rows {
		var vals []Value
		for _, expr := range p.Exprs {
			val, err := expr.Eval(ctx, row, binds)
			if err != nil {
				return nil, err
			}
			vals = append(vals, val)
		}
		key := NewTuple(vals)

		duplicate := false
		for i, k := range keys {
			cmp, err := Compare(k, key)
			if cmp == 0 && err == nil {
				children[i] = append(children[i], row)
				duplicate = true
				break
			}
		}
		if !duplicate {
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

				v1, err := FromSQL(values[i])
				if err != nil {
					return nil, err
				}
				v2, err := FromSQL(val)
				if err != nil {
					return nil, err
				}

				cmp, err := Compare(v1, v2)
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

func (p *GroupPlan) String() string {
	var exprs []string
	for _, expr := range p.Exprs {
		exprs = append(exprs, expr.String())
	}
	return fmt.Sprintf("GroupPlan(%s)", strings.Join(exprs, ", "))
}
