package task

import (
	"context"
	"database/sql/driver"
	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser"
)

func NewTableBuilder(builder Builder) Builder {
	return Build(func(node sqlparser.SQLNode) (Task, error) {
		switch node := node.(type) {
		case *sqlparser.AliasedTableExpr:
			return builder.Build(node.Expr)
		case *sqlparser.ParenTableExpr:
			return nil, driver.ErrSkip
		case *sqlparser.JoinTableExpr:
			return nil, driver.ErrSkip

		case sqlparser.TableName:
			return Run(func(ctx context.Context, value any) (any, error) {
				s, ok := value.(schema.Schema)
				if !ok {
					return nil, driver.ErrSkip
				}
				tbl, ok := s.Table(node.Name.CompliantName())
				if !ok {
					return nil, driver.ErrSkip
				}
				return tbl, nil
			}), nil

		case *sqlparser.Subquery:
			return builder.Build(node.Select)
		}
		return nil, driver.ErrSkip
	})
}
