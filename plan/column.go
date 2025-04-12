package plan

import (
	"context"
	"fmt"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type Column struct {
	Value *sqlparser.ColName
}

var _ Expr = (*Column)(nil)

func (p *Column) Eval(_ context.Context, row schema.Row, _ map[string]*querypb.BindVariable) (*schema.Value, error) {
	for i, col := range row.Columns {
		if col.Equal(p.Value) {
			val := row.Values[i]
			return &schema.Value{Type: val.Type(), Value: val.Raw()}, nil
		}
	}
	return schema.Null, nil
}

func (p *Column) String() string {
	return fmt.Sprintf("Column(%s)", sqlparser.String(p.Value))
}
