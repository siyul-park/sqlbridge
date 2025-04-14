package eval

import (
	"context"
	"fmt"
	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type Columns struct {
	Value sqlparser.TableName
}

var _ Expr = (*Columns)(nil)

func (e *Columns) Eval(_ context.Context, row schema.Row, _ map[string]*querypb.BindVariable) (Value, error) {
	var vals []Value
	for i, col := range row.Columns {
		if !e.Value.IsEmpty() && col.Qualifier != e.Value {
			continue
		}
		val, err := FromSQL(row.Values[i])
		if err != nil {
			return nil, err
		}
		vals = append(vals, val)
	}
	return NewTuple(vals), nil
}

func (e *Columns) String() string {
	return fmt.Sprintf("Columns(%s)", e.Value.Name)
}
