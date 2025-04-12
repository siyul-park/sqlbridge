package plan

import (
	"context"
	"fmt"
	"reflect"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type Equal struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*Equal)(nil)

func (p *Equal) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (*schema.Value, error) {
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
	rhs, err := Unmarshal(right.Type, right.Value)
	if err != nil {
		return nil, err
	}

	if reflect.DeepEqual(Promote(lhs, rhs)) {
		return schema.True, nil
	}
	return schema.False, nil
}

func (p *Equal) String() string {
	return fmt.Sprintf("Equal(%s, %s)", p.Left.String(), p.Right.String())
}
