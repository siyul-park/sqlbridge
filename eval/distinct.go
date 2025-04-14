package eval

import (
	"context"
	"fmt"
	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type Distinct struct {
	Input Expr
}

var _ Expr = (*Distinct)(nil)

func (e *Distinct) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	input, err := e.Input.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	val, ok := input.(*Tuple)
	if !ok {
		return val, nil
	}

	var vals []Value
	for _, val := range val.Values() {
		duplicate := true
		for _, v := range vals {
			cmp, err := Compare(v, val)
			if cmp == 0 && err == nil {
				duplicate = false
				break
			}
		}
		if !duplicate {
			continue
		}
		vals = append(vals, val)
	}
	return NewTuple(vals), nil

}

func (e *Distinct) String() string {
	return fmt.Sprintf("Distinct(%s)", e.Input.String())
}
