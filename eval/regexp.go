package eval

import (
	"context"
	"fmt"
	"regexp"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type Regexp struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*Regexp)(nil)

func (e *Regexp) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	left, err := e.Left.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	right, err := e.Right.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}

	lhs, err := ToString(left)
	if err != nil {
		return nil, err
	}
	rhs, err := ToString(right)
	if err != nil {
		return nil, err
	}

	re, err := regexp.Compile(rhs)
	if err != nil {
		return nil, err
	}

	return NewBool(re.MatchString(lhs)), nil
}

func (e *Regexp) String() string {
	return fmt.Sprintf("Regexp(%s, %s)", e.Left.String(), e.Right.String())
}
