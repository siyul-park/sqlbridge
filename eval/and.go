package eval

import (
	"context"
	"fmt"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type And struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*And)(nil)

func (p *And) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	left, err := p.Left.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	if !ToBool(left) {
		return False, nil
	}

	right, err := p.Right.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	return NewBool(ToBool(right)), nil
}

func (p *And) String() string {
	return fmt.Sprintf("And(%s, %s)", p.Left.String(), p.Right.String())
}
