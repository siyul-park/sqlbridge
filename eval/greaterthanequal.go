package eval

import (
	"context"
	"fmt"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type GreaterThanEqual struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*GreaterThanEqual)(nil)

func (p *GreaterThanEqual) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	left, err := p.Left.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	right, err := p.Right.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}

	cmp, err := Compare(left, right)
	if err != nil {
		return nil, err
	}
	return NewBool(cmp >= 0), nil
}

func (p *GreaterThanEqual) String() string {
	return fmt.Sprintf("GreaterThanEqual(%s, %s)", p.Left.String(), p.Right.String())
}
