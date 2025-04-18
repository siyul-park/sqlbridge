package eval

import (
	"context"
	"fmt"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type In struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*In)(nil)

func (e *In) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	left, err := e.Left.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	right, err := e.Right.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}

	if r, ok := right.(*Tuple); ok {
		for _, val := range r.Values() {
			if cmp, err := Compare(left, val); err != nil {
				return nil, err
			} else if cmp == 0 {
				return True, nil
			}
		}
	} else {
		if cmp, err := Compare(left, right); err != nil {
			return nil, err
		} else if cmp == 0 {
			return True, nil
		}
	}
	return False, nil
}

func (e *In) String() string {
	return fmt.Sprintf("In(%s, %s)", e.Left.String(), e.Right.String())
}
