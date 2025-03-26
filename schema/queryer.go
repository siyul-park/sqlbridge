package schema

import (
	"context"
	"database/sql/driver"
	"github.com/xwb1989/sqlparser"
)

type Queryer interface {
	Query(ctx context.Context, node sqlparser.SQLNode) (driver.Rows, error)
}

type Query func(ctx context.Context, node sqlparser.SQLNode) (driver.Rows, error)

var _ Queryer = (Query)(nil)

func (f Query) Query(ctx context.Context, node sqlparser.SQLNode) (driver.Rows, error) {
	return f(ctx, node)
}
