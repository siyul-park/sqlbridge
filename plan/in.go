package plan

import (
	"context"
	"fmt"
	"reflect"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type In struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*In)(nil)

func (p *In) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (*EvalResult, error) {
	left, err := p.Left.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	lhs, err := Unmarshal(left.Type, left.Value)
	if err != nil {
		return nil, err
	}

	right, err := p.Right.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}

	if right.Type == querypb.Type_TUPLE {
		for _, val := range right.Values {
			rhs, err := Unmarshal(val.Type, val.Value)
			if err != nil {
				return nil, err
			}
			if reflect.DeepEqual(Promote(rhs, lhs)) {
				return TRUE, nil
			}
		}
	} else {
		rhs, err := Unmarshal(right.Type, right.Value)
		if err != nil {
			return nil, err
		}
		if reflect.DeepEqual(Promote(rhs, lhs)) {
			return TRUE, nil
		}
	}

	return FALSE, nil
}

func (p *In) String() string {
	return fmt.Sprintf("In(%s, %s)", p.Left.String(), p.Right.String())
}
