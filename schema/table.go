package schema

import (
	"context"
	"database/sql/driver"
	"github.com/xwb1989/sqlparser"
)

type Table interface {
	Name() string
}

type Execer interface {
	Exec(ctx context.Context, node sqlparser.SQLNode) (driver.Result, error)
}

type Queryer interface {
	Query(ctx context.Context, node sqlparser.SQLNode) (driver.Rows, error)
}
