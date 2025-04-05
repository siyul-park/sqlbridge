package task

import (
	"context"
	"database/sql/driver"
	"fmt"
	"reflect"
	"sort"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser"
)

func NewSelectBuilder(builder Builder) Builder {
	return Build(func(node sqlparser.SQLNode) (Task, error) {
		switch n := node.(type) {
		case sqlparser.SelectExprs:
			tasks := make([]Task, 0, len(n))
			for _, expr := range n {
				task, err := builder.Build(expr)
				if err != nil {
					return nil, err
				}
				tasks = append(tasks, task)
			}

			return Run(func(ctx context.Context, value any) (any, error) {
				rows, ok := value.(driver.Rows)
				if !ok {
					return nil, NewErrUnsupportedType(value)
				}

				records, err := schema.ScanRows(rows)
				if err != nil {
					return nil, err
				}

				var columns [][]string
				var values [][]driver.Value
				for _, record := range records {
					var cols []string
					var vals []driver.Value
					for _, task := range tasks {
						val, err := task.Run(ctx, record)
						if err != nil {
							return nil, err
						}
						record, ok := val.(map[*sqlparser.ColName]driver.Value)
						if !ok {
							return nil, NewErrUnsupportedType(value)
						}
						for col, val := range record {
							cols = append(cols, sqlparser.String(col))
							vals = append(vals, val)
						}
					}
					columns = append(columns, cols)
					values = append(values, vals)
				}
				return schema.NewInlineRows(columns, values), nil
			}), nil

		case *sqlparser.StarExpr:
			return Run(func(ctx context.Context, value any) (any, error) {
				record, ok := value.(map[*sqlparser.ColName]driver.Value)
				if !ok {
					return nil, NewErrUnsupportedType(value)
				}
				return record, nil
			}), nil

		case *sqlparser.AliasedExpr:
			task, err := builder.Build(n.Expr)
			if err != nil {
				return nil, err
			}

			return Run(func(ctx context.Context, value any) (any, error) {
				record, ok := value.(map[*sqlparser.ColName]driver.Value)
				if !ok {
					return nil, NewErrUnsupportedType(value)
				}

				var col string
				if !n.As.IsEmpty() {
					col = n.As.String()
				} else {
					col = sqlparser.String(n.Expr)
				}

				val, err := task.Run(ctx, record)
				if err != nil {
					return nil, err
				}
				return map[*sqlparser.ColName]driver.Value{&sqlparser.ColName{Name: sqlparser.NewColIdent(col)}: val}, nil
			}), nil

		case *sqlparser.Where:
			task, err := builder.Build(n.Expr)
			if err != nil {
				return nil, err
			}

			return Run(func(ctx context.Context, value any) (any, error) {
				rows, ok := value.(driver.Rows)
				if !ok {
					return nil, NewErrUnsupportedType(value)
				}

				records, err := schema.ScanRows(rows)
				if err != nil {
					return nil, err
				}

				for i := 0; i < len(records); i++ {
					val, err := task.Run(ctx, records[i])
					if err != nil {
						return nil, err
					}
					if ok, _ := val.(bool); !ok {
						records = append(records[:i], records[i+1:]...)
						i--
					}
				}
				return schema.FormatRows(records), nil
			}), nil

		case sqlparser.OrderBy:
			tasks := make([]Task, 0, len(n))
			for _, order := range n {
				task, err := builder.Build(order.Expr)
				if err != nil {
					return nil, err
				}
				tasks = append(tasks, task)
			}

			return Run(func(ctx context.Context, value any) (any, error) {
				rows, ok := value.(driver.Rows)
				if !ok {
					return nil, NewErrUnsupportedType(value)
				}

				for _, task := range tasks {
					val, err := task.Run(ctx, rows)
					if err != nil {
						return nil, err
					}
					if rows, ok = val.(driver.Rows); !ok {
						return nil, NewErrUnsupportedType(val)
					}
				}
				return rows, nil
			}), nil

		case *sqlparser.Order:
			task, err := builder.Build(n.Expr)
			if err != nil {
				return nil, err
			}

			return Run(func(ctx context.Context, value any) (any, error) {
				rows, ok := value.(driver.Rows)
				if !ok {
					return nil, NewErrUnsupportedType(value)
				}

				records, err := schema.ScanRows(rows)
				if err != nil {
					return nil, err
				}

				sort.SliceStable(records, func(i, j int) bool {
					vi, err := task.Run(ctx, records[i])
					if err != nil {
						return false
					}
					vj, err := task.Run(ctx, records[j])
					if err != nil {
						return false
					}

					switch li := vi.(type) {
					case int64:
						if rj, ok := vj.(int64); ok {
							if li == rj {
								return false
							}
							if n.Direction == sqlparser.DescScr {
								return li > rj
							}
							return li < rj
						}
					case float64:
						if rj, ok := vj.(float64); ok {
							if li == rj {
								return false
							}
							if n.Direction == sqlparser.DescScr {
								return li > rj
							}
							return li < rj
						}
					case string:
						if rj, ok := vj.(string); ok {
							if li == rj {
								return false
							}
							if n.Direction == sqlparser.DescScr {
								return li > rj
							}
							return li < rj
						}
					}
					return false
				})
				return schema.FormatRows(records), nil
			}), nil

		case *sqlparser.Limit:
			var offset Task
			if n.Offset != nil {
				var err error
				offset, err = builder.Build(n.Offset)
				if err != nil {
					return nil, err
				}
			}

			var rowcount Task
			if n.Rowcount != nil {
				var err error
				rowcount, err = builder.Build(n.Rowcount)
				if err != nil {
					return nil, err
				}
			}

			return Run(func(ctx context.Context, value any) (any, error) {
				rows, ok := value.(driver.Rows)
				if !ok {
					return nil, NewErrUnsupportedType(value)
				}

				records, err := schema.ScanRows(rows)
				if err != nil {
					return nil, err
				}

				off := 0
				if offset != nil {
					val, err := offset.Run(ctx, nil)
					if err != nil {
						return nil, err
					}
					if v, ok := val.(int); ok {
						off = v
					}
				}

				cnt := len(records)
				if rowcount != nil {
					val, err := rowcount.Run(ctx, nil)
					if err != nil {
						return nil, err
					}
					if v, ok := val.(int); ok {
						cnt = v
					}
				}

				if off < len(records) {
					end := off + cnt
					if end > len(records) {
						end = len(records)
					}
					records = records[off:end]
				} else {
					records = nil
				}

				return schema.FormatRows(records), nil
			}), nil

		case *sqlparser.Select:
			var tasks []Task

			from, err := builder.Build(n.From)
			if err != nil {
				return nil, err
			}
			tasks = append(tasks, Run(func(ctx context.Context, value any) (any, error) {
				src, err := from.Run(ctx, value)
				if err != nil {
					return nil, err
				}
				var rows driver.Rows
				switch v := src.(type) {
				case driver.Rows:
					rows = v
				case schema.Queryer:
					rows, err = v.Query(ctx, n)
					if err != nil {
						return nil, err
					}
				default:
					return nil, fmt.Errorf("sqlbridge: unsupported types %T", src)
				}
				return rows, nil
			}))

			if n.Where != nil {
				where, err := builder.Build(n.Where)
				if err != nil {
					return nil, err
				}
				tasks = append(tasks, where)
			}
			if len(n.OrderBy) > 0 {
				orderBy, err := builder.Build(n.OrderBy)
				if err != nil {
					return nil, err
				}
				tasks = append(tasks, orderBy)
			}
			if n.Limit != nil {
				limit, err := builder.Build(n.Limit)
				if err != nil {
					return nil, err
				}
				tasks = append(tasks, limit)
			}
			if len(n.SelectExprs) > 0 {
				project, err := builder.Build(n.SelectExprs)
				if err != nil {
					return nil, err
				}
				tasks = append(tasks, project)
			}
			if n.Distinct == sqlparser.DistinctStr {
				tasks = append(tasks, Run(func(ctx context.Context, value any) (any, error) {
					rows, ok := value.(driver.Rows)
					if !ok {
						return nil, NewErrUnsupportedType(value)
					}

					records, err := schema.ScanRows(rows)
					if err != nil {
						return nil, err
					}

					for i := 0; i < len(records); i++ {
						duplicate := false
						for j := i + 1; j < len(records); j++ {
							if len(records[i]) != len(records[j]) {
								continue
							}
							match := true
							for k, v := range records[i] {
								if !reflect.DeepEqual(v, records[j][k]) {
									match = false
									break
								}
							}
							if match {
								duplicate = true
								break
							}
						}
						if duplicate {
							records = append(records[:i], records[i+1:]...)
							i--
						}
					}
					return schema.FormatRows(records), nil
				}))
			}

			return Run(func(ctx context.Context, value any) (any, error) {
				for _, task := range tasks {
					var err error
					value, err = task.Run(ctx, value)
					if err != nil {
						return nil, err
					}
				}
				return value, nil
			}), nil
		}
		return nil, driver.ErrSkip
	})
}
