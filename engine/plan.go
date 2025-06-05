package engine

import (
	"context"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type Plan interface {
	Run(ctx context.Context, binds map[string]*querypb.BindVariable) (schema.Cursor, error)
	String() string
}
