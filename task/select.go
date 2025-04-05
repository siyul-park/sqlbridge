package task

import (
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/siyul-park/sqlbridge/schema"
	"sort"
	"strings"

	"github.com/xwb1989/sqlparser"
)

func NewSelectTask(builder Builder) Builder {
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
				records, ok := value.([]map[*sqlparser.ColName]driver.Value)
				if !ok {
					return nil, fmt.Errorf("sqlbridge: unsupported types %T", value)
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
							return nil, fmt.Errorf("sqlbridge: unsupported types %T", value)
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
					return nil, fmt.Errorf("sqlbridge: unsupported types %T", value)
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
					return nil, fmt.Errorf("sqlbridge: unsupported types %T", value)
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
				records, ok := value.([]map[*sqlparser.ColName]driver.Value)
				if !ok {
					return nil, fmt.Errorf("sqlbridge: unsupported types %T", value)
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
				return records, nil
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
				records, ok := value.([]map[*sqlparser.ColName]driver.Value)
				if !ok {
					return nil, fmt.Errorf("sqlbridge: unsupported types %T", value)
				}
				for _, task := range tasks {
					val, err := task.Run(ctx, records)
					if err != nil {
						return nil, err
					}
					if records, ok = val.([]map[*sqlparser.ColName]driver.Value); !ok {
						return nil, fmt.Errorf("sqlbridge: unsupported types %T", val)
					}
				}
				return records, nil
			}), nil

		case *sqlparser.Order:
			task, err := builder.Build(n.Expr)
			if err != nil {
				return nil, err
			}

			return Run(func(ctx context.Context, value any) (any, error) {
				records, ok := value.([]map[*sqlparser.ColName]driver.Value)
				if !ok {
					return nil, fmt.Errorf("sqlbridge: unsupported types %T", value)
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
				return records, nil
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
				records, ok := value.([]map[*sqlparser.ColName]driver.Value)
				if !ok {
					return nil, fmt.Errorf("sqlbridge: unsupported types %T", value)
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

				return records, nil
			}), nil

		case *sqlparser.Select:
			project, err := builder.Build(n.SelectExprs)
			if err != nil {
				return nil, err
			}

			from, err := builder.Build(n.From)
			if err != nil {
				return nil, err
			}

			var tasks []Task
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

			return Run(func(ctx context.Context, value any) (any, error) {
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
				case schema.Table:
					rows, err = v.Rows(ctx)
					if err != nil {
						return nil, err
					}
				default:
					return nil, fmt.Errorf("sqlbridge: unsupported types %T", src)
				}

				var records []map[*sqlparser.ColName]driver.Value
				for {
					cols := rows.Columns()
					vals := make([]driver.Value, len(cols))
					if err := rows.Next(vals); err != nil {
						break
					}

					record := make(map[*sqlparser.ColName]driver.Value)
					for i, col := range cols {
						parts := strings.Split(col, ".")
						name := &sqlparser.ColName{Name: sqlparser.NewColIdent(parts[len(parts)-1])}
						if len(parts) > 1 {
							name.Qualifier = sqlparser.TableName{Qualifier: sqlparser.NewTableIdent(parts[0])}
						}
						record[name] = vals[i]
					}
					records = append(records, record)
				}

				for _, task := range tasks {
					val, err := task.Run(ctx, records)
					if err != nil {
						return nil, err
					}
					var ok bool
					if records, ok = val.([]map[*sqlparser.ColName]driver.Value); !ok {
						return nil, fmt.Errorf("sqlbridge: unsupported types %T", val)
					}
				}

				return project.Run(ctx, records)
			}), nil
		}
		return nil, driver.ErrSkip
	})
}
