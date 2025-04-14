package eval

import (
	"context"
	"fmt"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type If struct {
	When Expr
	Then Expr
	Else Expr
}

var _ Expr = (*If)(nil)

func (e *If) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	cond, err := e.When.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	if ToBool(cond) {
		return e.Then.Eval(ctx, row, binds)
	}
	if e.Else != nil {
		return e.Else.Eval(ctx, row, binds)
	}
	return False, nil
}

func (e *If) String() string {
	return fmt.Sprintf("If(%s, %s, %s)", e.When.String(), e.Then.String(), e.Else.String())
}
