package plan

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

func (p *Not) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (*EvalResult, error) {
	val, err := p.Input.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	v, err := Unmarshal(val.Type, val.Value)
	if err != nil {
		return nil, err
	}
	if !ToBool(v) {
		return TRUE, nil
	}
	return FALSE, nil
}

func (p *Not) String() string {
	return fmt.Sprintf("Not(%s)", p.Input.String())
}
