package eval

import (
	"context"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type Expr interface {
	Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error)
	String() string
}
