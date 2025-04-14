package eval

import (
	"context"
	"fmt"
	"strings"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type Values struct {
	Exprs []Expr
}

var _ Expr = (*Values)(nil)

func (v *Values) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	var vals []Value
	for _, elem := range v.Exprs {
		val, err := elem.Eval(ctx, row, binds)
		if err != nil {
			return nil, err
		}
		switch val := val.(type) {
		case *Tuple:
			vals = append(vals, val.Values()...)
		default:
			vals = append(vals, val)
		}
	}
	if len(vals) == 1 {
		return vals[0], nil
	}
	return NewTuple(vals), nil
}

func (v *Values) String() string {
	parts := make([]string, len(v.Exprs))
	for i, e := range v.Exprs {
		parts[i] = e.String()
	}
	return fmt.Sprintf("Values(%s)", strings.Join(parts, ", "))
}
