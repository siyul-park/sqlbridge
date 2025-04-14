package eval

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

func (e *Column) Eval(_ context.Context, row schema.Row, _ map[string]*querypb.BindVariable) (Value, error) {
	for i, col := range row.Columns {
		if col.Equal(e.Value) {
			return FromSQL(row.Values[i])
		}
	}
	return nil, nil
}

func (e *Column) String() string {
	return fmt.Sprintf("Columns(%s)", sqlparser.String(e.Value))
}
