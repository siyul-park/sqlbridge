package eval

import (
	"context"
	"fmt"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type LessThanEqual struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*LessThanEqual)(nil)

func (p *LessThanEqual) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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
	return NewBool(cmp <= 0), nil
}

func (p *LessThanEqual) String() string {
	return fmt.Sprintf("LessThanEqual(%s, %s)", p.Left.String(), p.Right.String())
}
