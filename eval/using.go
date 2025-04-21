package eval

import (
	"context"
	"fmt"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type Using struct {
	Value sqlparser.ColIdent
}

var _ Expr = (*Using)(nil)

func (e *Using) Eval(_ context.Context, row schema.Row, _ map[string]*querypb.BindVariable) (Value, error) {
	var values []Value
	for i, col := range row.Columns {
		if col.Name.Equal(e.Value) {
			value, err := FromSQL(row.Values[i])
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
	}
	for i := 0; i < len(values); i++ {
		for j := i + 1; j < len(values); j++ {
			if cmp, err := Compare(values[i], values[j]); err != nil {
				return nil, err
			} else if cmp != 0 {
				return False, nil
			}
		}
	}
	return True, nil
}

func (e *Using) String() string {
	return fmt.Sprintf("Using(%s)", sqlparser.String(e.Value))
}
