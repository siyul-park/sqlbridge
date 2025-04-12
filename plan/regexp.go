package plan

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

func (p *Regexp) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (*querypb.BindVariable, error) {
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

	lstr := ToString(lhs)
	rstr := ToString(rhs)

	re, err := regexp.Compile(rstr)
	if err != nil {
		return nil, err
	}

	if re.MatchString(lstr) {
		return TRUE, nil
	}
	return FALSE, nil
}

func (p *Regexp) String() string {
	return fmt.Sprintf("Regexp(%s, %s)", p.Left.String(), p.Right.String())
}
