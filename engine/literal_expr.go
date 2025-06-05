package engine

import (
	"context"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
)

type LiteralExpr struct {
	Value sqltypes.Value
}

var _ Expr = (*LiteralExpr)(nil)

func (e *LiteralExpr) Eval(_ context.Context, _ schema.Row, _ map[string]*querypb.BindVariable) (Value, error) {
	return FromSQL(e.Value)
}

func (e *LiteralExpr) String() string {
	return e.Value.String()
}
