package eval

import (
	"context"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
)

type Literal struct {
	Value sqltypes.Value
}

var _ Expr = (*Literal)(nil)

func (e *Literal) Eval(_ context.Context, _ schema.Row, _ map[string]*querypb.BindVariable) (Value, error) {
	return FromSQL(e.Value)
}

func (e *Literal) String() string {
	return e.Value.String()
}
