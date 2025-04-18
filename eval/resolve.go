package eval

import (
	"context"
	"fmt"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
)

type Resolve struct {
	Value string
}

var _ Expr = (*Resolve)(nil)

func (e *Resolve) Eval(_ context.Context, _ schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	val, ok := binds[e.Value]
	if !ok {
		return nil, nil
	}
	switch val.Type {
	case querypb.Type_TUPLE:
		vals := make([]Value, 0, len(val.Values))
		for _, v := range val.Values {
			val, err := FromSQL(sqltypes.MakeTrusted(v.Type, v.Value))
			if err != nil {
				return nil, err
			}
			vals = append(vals, val)
		}
		return NewTuple(vals), nil
	default:
		return FromSQL(sqltypes.MakeTrusted(val.Type, val.Value))
	}
}

func (e *Resolve) String() string {
	return fmt.Sprintf("Resolve(%s)", e.Value)
}
