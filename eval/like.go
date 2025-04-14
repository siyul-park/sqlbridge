package eval

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

func (e *Like) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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

	pattern := "^" + regexp.QuoteMeta(rhs) + "$"
	pattern = strings.ReplaceAll(pattern, `%`, `.*`)
	pattern = strings.ReplaceAll(pattern, `_`, `.`)

	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}

	return NewBool(re.MatchString(lhs)), nil
}

func (e *Like) String() string {
	return fmt.Sprintf("Like(%s, %s)", e.Left.String(), e.Right.String())
}
