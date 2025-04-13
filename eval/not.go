package eval

import (
	"context"
	"fmt"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type Not struct {
	Input Expr
}

var _ Expr = (*Not)(nil)

func (p *Not) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	val, err := p.Input.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	return NewBool(!ToBool(val)), nil
}

func (p *Not) String() string {
	return fmt.Sprintf("Not(%s)", p.Input.String())
}
