package plan

import (
	"context"
	"fmt"

	"github.com/siyul-park/sqlbridge/eval"
	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type Distinct struct {
	Input Plan
}

var _ Plan = (*Distinct)(nil)

func (p *Distinct) Run(ctx context.Context, binds map[string]*querypb.BindVariable) (schema.Cursor, error) {
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

				v1, err := eval.FromSQL(val1)
				if err != nil {
					return schema.Row{}, err
				}
				v2, err := eval.FromSQL(val2)
				if err != nil {
					return schema.Row{}, err
				}

				cmp, err := eval.Compare(v1, v2)
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

func (p *Distinct) String() string {
	return fmt.Sprintf("Distinct(%s)", p.Input.String())
}
