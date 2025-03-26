package schema

import (
	"context"
	"database/sql/driver"
	"github.com/xwb1989/sqlparser"
)

type Execer interface {
	Exec(ctx context.Context, node sqlparser.SQLNode) (driver.Result, error)
}

type Exec func(ctx context.Context, node sqlparser.SQLNode) (driver.Result, error)

var _ Execer = (Exec)(nil)

func (f Exec) Exec(ctx context.Context, node sqlparser.SQLNode) (driver.Result, error) {
	return f(ctx, node)
}
