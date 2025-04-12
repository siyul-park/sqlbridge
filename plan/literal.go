package plan

import (
	"context"

	"github.com/xwb1989/sqlparser/dependency/sqltypes"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type Literal struct {
	Value sqltypes.Value
}

var _ Expr = (*Literal)(nil)

func (e *Literal) Eval(_ context.Context, _ schema.Row, _ map[string]*querypb.BindVariable) (*schema.Value, error) {
	return &schema.Value{Type: e.Value.Type(), Value: e.Value.Raw()}, nil
}

func (e *Literal) String() string {
	return e.Value.String()
}
