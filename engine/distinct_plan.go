package engine

import (
	"context"
	"fmt"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type DistinctPlan struct {
	Input Plan
}

var _ Plan = (*DistinctPlan)(nil)

func (p *DistinctPlan) Run(ctx context.Context, binds map[string]*querypb.BindVariable) (schema.Cursor, error) {
	cursor, err := p.Input.Run(ctx, binds)
	if err != nil {
		return nil, err
	}

	var rows []schema.Row
	return schema.NewMappedCursor(cursor, func(row schema.Row) (schema.Row, error) {
		for _, r := range rows {
			if len(r.Columns) != len(row.Columns) {
				continue
			}

			duplicate := true
			for _, col := range r.Columns {
				val1, ok1 := r.Get(col)
				val2, ok2 := row.Get(col)
				if !ok1 || !ok2 {
					duplicate = false
					break
				}

				v1, err := FromSQL(val1)
				if err != nil {
					return schema.Row{}, err
				}
				v2, err := FromSQL(val2)
				if err != nil {
					return schema.Row{}, err
				}

				cmp, err := Compare(v1, v2)
				if err != nil || cmp != 0 {
					duplicate = false
					break
				}
			}
			if duplicate {
				return schema.Row{}, nil
			}
		}

		rows = append(rows, row)
		return row, nil
	}), nil
}

func (p *DistinctPlan) Walk(f func(Plan) (bool, error)) (bool, error) {
	if cont, err := f(p); !cont || err != nil {
		return cont, err
	}
	return p.Input.Walk(f)
}

func (p *DistinctPlan) String() string {
	return fmt.Sprintf("DistinctPlan(%s)", p.Input.String())
}
