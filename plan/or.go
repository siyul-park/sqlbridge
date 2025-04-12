package plan

import (
	"context"
	"fmt"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type Or struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*Or)(nil)

func (p *Or) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (*schema.Value, error) {
	left, err := p.Left.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	lhs, err := Unmarshal(left.Type, left.Value)
	if err != nil {
		return nil, err
	}
	if ToBool(lhs) {
		return schema.True, nil
	}

	right, err := p.Right.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	rhs, err := Unmarshal(right.Type, right.Value)
	if err != nil {
		return nil, err
	}
	if ToBool(rhs) {
		return schema.True, nil
	}

	return schema.False, nil
}

func (p *Or) String() string {
	return fmt.Sprintf("Or(%s, %s)", p.Left.String(), p.Right.String())
}
