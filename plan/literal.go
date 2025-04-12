package plan

import (
	"context"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type Literal struct {
	Value *querypb.BindVariable
}

var _ Expr = (*Literal)(nil)

func (e *Literal) Eval(_ context.Context, _ schema.Row, _ map[string]*querypb.BindVariable) (*querypb.BindVariable, error) {
	return e.Value, nil
}

func (e *Literal) String() string {
	return e.Value.String()
}
