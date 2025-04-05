package task

import (
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser"
	"sort"
)

func NewSelectTask(builder Builder) Builder {
	return Build(func(node sqlparser.SQLNode) (Task, error) {
		switch node := node.(type) {
		case *sqlparser.Select:
			parts := Partition(node)

			sources := make(map[sqlparser.TableIdent]Task, len(parts))
			for tbl, sel := range parts {
				if len(sel.From) != 1 {
					return nil, driver.ErrSkip
				}
				task, err := builder.Build(sel.From[0])
				if err != nil {
					return nil, err
				}
				sources[tbl] = task
			}

			return Run(func(ctx context.Context, value any) (any, error) {
				tables := make(map[sqlparser.TableIdent][]map[string]driver.Value, len(sources))
				for tbl, task := range sources {
					val, err := task.Run(ctx, value)
					if err != nil {
						return nil, err
					}

					if q, ok := val.(schema.Queryer); ok {
						rows, err := q.Query(ctx, node)
						if err != nil {
							return nil, err
						}

						var records []map[string]driver.Value
						for {
							values := make([]driver.Value, len(rows.Columns()))
							if err := rows.Next(values); err != nil {
								break
							}
							record := make(map[string]driver.Value, len(rows.Columns()))
							for i, col := range rows.Columns() {
								record[col] = values[i]
							}
							records = append(records, record)
						}
						_ = rows.Close()

						tables[tbl] = records
						continue
					}

					var rows driver.Rows
					if r, ok := val.(driver.Rows); ok {
						rows = r
					} else if t, ok := val.(schema.Table); ok {
						if rows, err = t.Rows(ctx); err != nil {
							return nil, err
						}
					} else {
						return nil, driver.ErrSkip
					}

					sel := parts[tbl]

					columns := rows.Columns()
					var filtered []map[string]driver.Value
					for {
						row := make([]driver.Value, len(columns))
						if err := rows.Next(row); err != nil {
							break
						}

						record := make(map[string]driver.Value, len(columns))
						for i, column := range columns {
							record[column] = row[i]
						}

						if sel.Where != nil {
							cond, err := builder.Build(sel.Where.Expr)
							if err != nil {
								_ = rows.Close()
								return nil, err
							}
							val, err := cond.Run(ctx, record)
							if err != nil {
								_ = rows.Close()
								return nil, err
							}
							if val, ok := val.(bool); !ok || !val {
								continue
							}
						}
						filtered = append(filtered, record)
					}
					_ = rows.Close()

					if len(sel.GroupBy) > 0 {
						grouped := map[string][]map[string]driver.Value{}
						for _, record := range filtered {
							key := ""
							for _, expr := range sel.GroupBy {
								column, err := builder.Build(expr)
								if err != nil {
									return nil, err
								}
								val, err := column.Run(ctx, record)
								if err != nil {
									return nil, err
								}
								key += fmt.Sprintf("|%v", val)
							}
							grouped[key] = append(grouped[key], record)
						}

						filtered = nil
						for _, group := range grouped {
							env := map[string]driver.Value{"_group": group}
							for _, record := range group {
								for k, v := range record {
									env[k] = v
								}
							}

							if sel.Having != nil {
								cond, err := builder.Build(sel.Having.Expr)
								if err != nil {
									return nil, err
								}
								val, err := cond.Run(ctx, env)
								if err != nil {
									return nil, err
								}
								if val, ok := val.(bool); !ok || !val {
									continue
								}
							}

							filtered = append(filtered, env)
						}
					}

					var records []map[string]driver.Value
					for _, env := range filtered {
						record := make(map[string]driver.Value, len(sel.SelectExprs))
						for _, expr := range sel.SelectExprs {
							switch e := expr.(type) {
							case *sqlparser.AliasedExpr:
								var column string
								if !e.As.IsEmpty() {
									column = e.As.String()
								} else if col, ok := e.Expr.(*sqlparser.ColName); ok {
									column = col.Name.String()
								} else {
									column = "?"
								}

								t, err := builder.Build(e.Expr)
								if err != nil {
									return nil, err
								}
								val, err := t.Run(ctx, env)
								if err != nil {
									return nil, err
								}
								record[column] = val

							case *sqlparser.StarExpr:
								for _, column := range columns {
									record[column] = env[column]
								}

							case sqlparser.Nextval:
								t, err := builder.Build(e)
								if err != nil {
									return nil, err
								}
								val, err := t.Run(ctx, env)
								if err != nil {
									return nil, err
								}
								record["nextval"] = val

							default:
								return nil, driver.ErrSkip
							}
						}
						records = append(records, record)
					}

					if len(sel.OrderBy) > 0 {
						sort.SliceStable(records, func(i, j int) bool {
							for _, order := range sel.OrderBy {
								expr := order.Expr
								left, err := builder.Build(expr)
								if err != nil {
									continue
								}
								vi, err := left.Run(ctx, records[i])
								if err != nil {
									continue
								}
								vj, err := left.Run(ctx, records[j])
								if err != nil {
									continue
								}

								switch li := vi.(type) {
								case int64:
									if rj, ok := vj.(int64); ok {
										if li == rj {
											continue
										}
										if order.Direction == sqlparser.DescScr {
											return li > rj
										}
										return li < rj
									}
								case float64:
									if rj, ok := vj.(float64); ok {
										if li == rj {
											continue
										}
										if order.Direction == sqlparser.DescScr {
											return li > rj
										}
										return li < rj
									}
								case string:
									if rj, ok := vj.(string); ok {
										if li == rj {
											continue
										}
										if order.Direction == sqlparser.DescScr {
											return li > rj
										}
										return li < rj
									}
								}
							}
							return false
						})
					}

					if sel.Limit != nil {
						offset := 0
						count := len(records)

						if sel.Limit.Offset != nil {
							expr, err := builder.Build(sel.Limit.Offset)
							if err != nil {
								return nil, err
							}
							val, err := expr.Run(ctx, nil)
							if err != nil {
								return nil, err
							}
							if v, ok := val.(int); ok {
								offset = v
							}
						}

						if sel.Limit.Rowcount != nil {
							expr, err := builder.Build(sel.Limit.Rowcount)
							if err != nil {
								return nil, err
							}
							val, err := expr.Run(ctx, nil)
							if err != nil {
								return nil, err
							}
							if v, ok := val.(int); ok {
								count = v
							}
						}

						if offset < len(records) {
							end := offset + count
							if end > len(records) {
								end = len(records)
							}
							records = records[offset:end]
						} else {
							records = nil
						}
					}

					tables[tbl] = records
				}

				var records []map[string]driver.Value
				for i, expr := range node.From {
					switch expr := expr.(type) {
					case *sqlparser.AliasedTableExpr:
						var name sqlparser.TableIdent
						if !expr.As.IsEmpty() {
							name = expr.As
						}

						rows := tables[name]
						if i == 0 {
							for _, row := range rows {
								record := make(map[string]driver.Value)
								for k, v := range row {
									record[name.CompliantName()+k] = v
								}
								records = append(records, row)
							}
						} else {
							var joined []map[string]driver.Value
							for _, left := range records {
								for _, right := range rows {
									row := make(map[string]driver.Value)
									for k, v := range left {
										row[k] = v
									}
									for k, v := range right {
										row[name.CompliantName()+k] = v
									}
									joined = append(joined, row)
								}
							}
							records = joined
						}
					case *sqlparser.ParenTableExpr:
						return nil, driver.ErrSkip
					case *sqlparser.JoinTableExpr:
						return nil, driver.ErrSkip
					}
				}

				return schema.NewRows(records), nil
			}), nil
		}
		return nil, driver.ErrSkip
	})
}
