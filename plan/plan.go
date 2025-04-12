package plan

import (
	"context"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type Plan interface {
	Run(ctx context.Context, binds map[string]*querypb.BindVariable) (schema.Cursor, error)
	String() string
}

type Expr interface {
	Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (*querypb.BindVariable, error)
	String() string
}
