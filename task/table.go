package task

import (
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser"
	"strings"
)

func NewTableBuilder(builder Builder) Builder {
	return Build(func(node sqlparser.SQLNode) (Task, error) {
		switch n := node.(type) {
		case sqlparser.TableExprs:
			tasks := make([]Task, 0, len(n))
			for _, expr := range n {
				task, err := builder.Build(expr)
				if err != nil {
					return nil, err
				}
				tasks = append(tasks, task)
			}
			return Run(func(ctx context.Context, value any) (any, error) {
				vals := make([]any, 0, len(tasks))
				for _, task := range tasks {
					val, err := task.Run(ctx, value)
					if err != nil {
						return nil, err
					}
					vals = append(vals, val)
				}

				return schema.Query(func(ctx context.Context, node sqlparser.SQLNode) (driver.Rows, error) {
					var srcs []driver.Rows
					for _, val := range vals {
						switch v := val.(type) {
						case schema.Queryer:
							rows, err := v.Query(ctx, node)
							if err != nil {
								return nil, err
							}
							srcs = append(srcs, rows)
						case schema.Table:
							rows, err := v.Rows(ctx)
							if err != nil {
								return nil, err
							}
							srcs = append(srcs, rows)
						case driver.Rows:
							srcs = append(srcs, v)
						default:
							return nil, fmt.Errorf("sqlbridge: unsupported types %T", val)
						}
					}

					var columnsList [][][]string
					var valuesList [][][]driver.Value

					for _, src := range srcs {
						var columns [][]string
						var values [][]driver.Value

						for {
							cols := src.Columns()
							row := make([]driver.Value, len(cols))
							if err := src.Next(row); err != nil {
								break
							}
							columns = append(columns, cols)
							values = append(values, row)
						}
						_ = src.Close()

						columnsList = append(columnsList, columns)
						valuesList = append(valuesList, values)
					}

					joinColumns := [][]string{{}}
					joinValues := [][]driver.Value{{}}
					for i := range columnsList {
						var combineValues [][]driver.Value
						var combineColumns [][]string
						for j := range columnsList[i] {
							for k := range joinColumns {
								combineColumns = append(combineColumns, append(append([]string{}, joinColumns[k]...), columnsList[i][j]...))
								combineValues = append(combineValues, append(append([]driver.Value{}, joinValues[k]...), valuesList[i][j]...))
							}
						}
						joinColumns = combineColumns
						joinValues = combineValues
					}

					return schema.NewInlineRows(joinColumns, joinValues), nil
				}), nil
			}), nil

		case *sqlparser.AliasedTableExpr:
			qualifier := sqlparser.TableName{Name: n.As}

			task, err := builder.Build(n.Expr)
			if err != nil {
				return nil, err
			}

			alias := func(src driver.Rows, qualifier sqlparser.TableName) driver.Rows {
				var columns [][]string
				var values [][]driver.Value
				for {
					cols := src.Columns()
					row := make([]driver.Value, len(cols))
					if err := src.Next(row); err != nil {
						break
					}

					if !qualifier.IsEmpty() {
						prefix := qualifier.Name.String()
						for i, col := range cols {
							parts := strings.Split(col, ".")
							name := parts[len(parts)-1]
							cols[i] = prefix + "." + name
						}
					}

					columns = append(columns, cols)
					values = append(values, row)
				}
				_ = src.Close()
				return schema.NewInlineRows(columns, values)
			}

			return Run(func(ctx context.Context, value any) (any, error) {
				val, err := task.Run(ctx, value)
				if err != nil {
					return nil, err
				}

				switch v := val.(type) {
				case schema.Queryer:
					return schema.Query(func(ctx context.Context, node sqlparser.SQLNode) (driver.Rows, error) {
						n, ok := node.(*sqlparser.Select)
						if !ok {
							return nil, fmt.Errorf("unexpected node type: %T", node)
						}

						parts := Partition(n)
						if len(parts) == 0 {
							return nil, fmt.Errorf("no partition found for table %s", sqlparser.String(n))
						}

						rows, err := v.Query(ctx, parts[qualifier])
						if err != nil {
							return nil, err
						}

						return alias(rows, qualifier), nil
					}), nil
				case schema.Table:
					rows, err := v.Rows(ctx)
					if err != nil {
						return nil, err
					}
					return alias(rows, qualifier), nil
				case driver.Rows:
					return alias(v, qualifier), nil
				default:
					return nil, fmt.Errorf("sqlbridge: unsupported types %T", val)
				}
			}), nil

		case *sqlparser.ParenTableExpr:
			return builder.Build(n.Exprs)

		case *sqlparser.JoinTableExpr:
			return nil, driver.ErrSkip

		case sqlparser.TableName:
			return Run(func(ctx context.Context, value any) (any, error) {
				s, ok := value.(schema.Schema)
				if !ok {
					return nil, driver.ErrSkip
				}
				tbl, ok := s.Table(n.Name.CompliantName())
				if !ok {
					return nil, driver.ErrSkip
				}
				return tbl, nil
			}), nil

		case *sqlparser.Subquery:
			return builder.Build(n.Select)
		}
		return nil, driver.ErrSkip
	})
}
