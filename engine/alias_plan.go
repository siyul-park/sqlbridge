package engine

import (
	"context"
	"fmt"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type AliasPlan struct {
	Input Plan
	As    sqlparser.TableIdent
}

var _ Plan = (*AliasPlan)(nil)

func (p *AliasPlan) Run(ctx context.Context, binds map[string]*querypb.BindVariable) (schema.Cursor, error) {
	input, err := p.Input.Run(ctx, binds)
	if err != nil {
		return nil, err
	}
	return schema.NewMappedCursor(input, func(row schema.Row) (schema.Row, error) {
		columns := make([]*sqlparser.ColName, 0, len(row.Columns))
		for _, col := range row.Columns {
			columns = append(columns, &sqlparser.ColName{
				Metadata:  col.Metadata,
				Name:      col.Name,
				Qualifier: sqlparser.TableName{Qualifier: col.Qualifier.Qualifier, Name: p.As},
			})
		}
		row.Columns = columns
		return row, nil
	}), nil
}

func (p *AliasPlan) String() string {
	return fmt.Sprintf("AliasPlan(%s, %s)", p.Input.String(), sqlparser.String(p.As))
}
