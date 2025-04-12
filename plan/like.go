package plan

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type Like struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*Like)(nil)

func (p *Like) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (*schema.Value, error) {
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

	pattern := "^" + regexp.QuoteMeta(rstr) + "$"
	pattern = strings.ReplaceAll(pattern, `%`, `.*`)
	pattern = strings.ReplaceAll(pattern, `_`, `.`)

	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}

	if re.MatchString(lstr) {
		return schema.True, nil
	}
	return schema.False, nil
}

func (p *Like) String() string {
	return fmt.Sprintf("Like(%s, %s)", p.Left.String(), p.Right.String())
}
