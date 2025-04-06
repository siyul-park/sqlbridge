package task

import (
	"context"
	"database/sql/driver"
	"reflect"
	"sort"
	"strings"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/siyul-park/sqlbridge/vm"
	"github.com/xwb1989/sqlparser"
)

type Task interface {
	Run(ctx context.Context) (driver.Value, error)
}

type NopTask struct{}

var _ Task = (*NopTask)(nil)

func (t *NopTask) Run(_ context.Context) (driver.Value, error) {
	return nil, nil
}

type ScanTask struct {
	Catalog schema.Catalog
	Table   sqlparser.TableName
}

var _ Task = (*NopTask)(nil)

func (t *ScanTask) Run(ctx context.Context) (driver.Value, error) {
	table, err := t.Catalog.Table(t.Table.Name.CompliantName())
	if err != nil {
		return nil, err
	}
	return table.Scan(ctx)
}

type AliasTask struct {
	Input Task
	Alias sqlparser.TableIdent
}

var _ Task = (*AliasTask)(nil)

func (t *AliasTask) Run(ctx context.Context) (driver.Value, error) {
	val, err := t.Input.Run(ctx)
	if err != nil {
		return nil, err
	}
	rows, ok := val.(driver.Rows)
	if !ok {
		return nil, NewErrUnsupportedType(val)
	}
	columns, values, err := schema.ReadAll(rows)
	if err != nil {
		return nil, err
	}

	if !t.Alias.IsEmpty() {
		for i := 0; i < len(columns); i++ {
			cols := columns[i]
			prefix := t.Alias.String()
			for i, col := range cols {
				parts := strings.Split(col, ".")
				name := parts[len(parts)-1]
				cols[i] = prefix + "." + name
			}
		}
	}
	return schema.NewInMemoryRows(columns, values), nil
}

type JoinTask struct {
	Left  Task
	Right Task
	Join  string
	On    sqlparser.Expr
	Using []sqlparser.ColIdent
}

var _ Task = (*JoinTask)(nil)

func (t *JoinTask) Run(ctx context.Context) (driver.Value, error) {
	leftValue, err := t.Left.Run(ctx)
	if err != nil {
		return nil, err
	}
	leftRows, ok := leftValue.(driver.Rows)
	if !ok {
		return nil, NewErrUnsupportedType(leftValue)
	}
	leftColumns, leftValues, err := schema.ReadAll(leftRows)
	if err != nil {
		return nil, err
	}

	rightValue, err := t.Right.Run(ctx)
	if err != nil {
		return nil, err
	}
	rightRows, ok := rightValue.(driver.Rows)
	if !ok {
		return nil, NewErrUnsupportedType(rightValue)
	}
	rightColumns, rightValues, err := schema.ReadAll(rightRows)
	if err != nil {
		return nil, err
	}

	var joinedColumns [][]string
	var joinedValues [][]driver.Value

	switch t.Join {
	case "", sqlparser.JoinStr:
		for i := range leftColumns {
			for j := range rightColumns {
				if ok, err := t.on(leftColumns[i], leftValues[i], rightColumns[i], rightValues[i]); err != nil {
					return nil, err
				} else if !ok {
					continue
				}

				joinedColumns = append(joinedColumns, append(leftColumns[i], rightColumns[j]...))
				joinedValues = append(joinedValues, append(leftValues[i], rightValues[j]...))
			}
		}
	default:
		return nil, NewErrUnsupportedValue(t.Join)
	}
	return schema.NewInMemoryRows(joinedColumns, joinedValues), nil
}

func (t *JoinTask) on(leftColumns []string, leftValues []driver.Value, rightColumns []string, rightValues []driver.Value) (bool, error) {
	if t.On != nil {
		columns := append(leftColumns, rightColumns...)
		values := append(leftValues, rightValues...)

		val, err := vm.Eval(schema.Bind(columns, values), t.On)
		if err != nil {
			return false, err
		}
		if !reflect.ValueOf(val).IsValid() || reflect.ValueOf(val).IsZero() {
			return false, nil
		}
	}

	for _, using := range t.Using {
		var lhs driver.Value
		for i := range leftColumns {
			if leftColumns[i] == using.String() || strings.HasSuffix(leftColumns[i], "."+using.String()) {
				lhs = leftValues[i]
			}
		}

		var rhs driver.Value
		for i := range rightColumns {
			if rightColumns[i] == using.String() || strings.HasSuffix(rightColumns[i], "."+using.String()) {
				rhs = rightValues[i]
			}
		}

		val, err := vm.Eval(map[string]driver.Value{"lhs": lhs, "rhs": rhs}, &sqlparser.ComparisonExpr{
			Operator: sqlparser.EqualStr,
			Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("lhs")},
			Right:    &sqlparser.ColName{Name: sqlparser.NewColIdent("rhs")},
		})
		if err != nil {
			return false, err
		}
		if !reflect.ValueOf(val).IsValid() || reflect.ValueOf(val).IsZero() {
			return false, nil
		}
	}
	return true, nil
}

type FilterTask struct {
	Input Task
	Expr  sqlparser.Expr
}

var _ Task = (*FilterTask)(nil)

func (t *FilterTask) Run(ctx context.Context) (driver.Value, error) {
	val, err := t.Input.Run(ctx)
	if err != nil {
		return nil, err
	}
	rows, ok := val.(driver.Rows)
	if !ok {
		return nil, NewErrUnsupportedType(val)
	}
	columns, values, err := schema.ReadAll(rows)
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(columns); i++ {
		val, err := vm.Eval(schema.Bind(columns[i], values[i]), t.Expr)
		if err != nil {
			return nil, err
		}

		if !reflect.ValueOf(val).IsValid() || reflect.ValueOf(val).IsZero() {
			columns = append(columns[:i], columns[i+1:]...)
			values = append(values[:i], values[i+1:]...)
			i--
		}
	}
	return schema.NewInMemoryRows(columns, values), nil
}

type ProjectTask struct {
	Input Task
	Exprs sqlparser.SelectExprs
}

var _ Task = (*ProjectTask)(nil)

func (t *ProjectTask) Run(ctx context.Context) (driver.Value, error) {
	val, err := t.Input.Run(ctx)
	if err != nil {
		return nil, err
	}
	rows, ok := val.(driver.Rows)
	if !ok {
		return nil, NewErrUnsupportedType(val)
	}
	columns, values, err := schema.ReadAll(rows)
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(columns); i++ {
		var cols []string
		var vals []driver.Value
		for _, expr := range t.Exprs {
			switch expr := expr.(type) {
			case *sqlparser.StarExpr:
				for j := 0; j < len(columns[i]); j++ {
					col := columns[i][j]
					val := values[i][j]

					if !expr.TableName.IsEmpty() && !strings.HasPrefix(col, expr.TableName.Name.CompliantName()+".") {
						continue
					}

					parts := strings.Split(col, ".")
					col = parts[len(parts)-1]

					cols = append(cols, col)
					vals = append(vals, val)
				}
			case *sqlparser.AliasedExpr:
				col := sqlparser.String(expr.Expr)
				if !expr.As.IsEmpty() {
					col = expr.As.String()
				}

				val, err := vm.Eval(schema.Bind(columns[i], values[i]), expr.Expr)
				if err != nil {
					return nil, err
				}

				cols = append(cols, col)
				vals = append(vals, val)
			default:
				return nil, NewErrUnsupportedValue(expr)
			}
		}

		columns[i] = cols
		values[i] = vals
	}
	return schema.NewInMemoryRows(columns, values), nil
}

type OrderTask struct {
	Input  Task
	Orders sqlparser.OrderBy
}

var _ Task = (*OrderTask)(nil)

func (t *OrderTask) Run(ctx context.Context) (driver.Value, error) {
	val, err := t.Input.Run(ctx)
	if err != nil {
		return nil, err
	}
	rows, ok := val.(driver.Rows)
	if !ok {
		return nil, NewErrUnsupportedType(val)
	}
	columns, values, err := schema.ReadAll(rows)
	if err != nil {
		return nil, err
	}

	records := make([]map[string]driver.Value, 0, len(columns))
	for i := 0; i < len(columns); i++ {
		records = append(records, schema.Bind(columns[i], values[i]))
	}

	for _, order := range t.Orders {
		sort.SliceStable(records, func(i, j int) bool {
			lhs, err := vm.Eval(records[i], order.Expr)
			if err != nil {
				return false
			}

			rhs, err := vm.Eval(records[j], order.Expr)
			if err != nil {
				return false
			}

			cmp := &sqlparser.ComparisonExpr{
				Operator: sqlparser.LessThanStr,
				Left:     &sqlparser.ColName{Name: sqlparser.NewColIdent("lhs")},
				Right:    &sqlparser.ColName{Name: sqlparser.NewColIdent("rhs")},
			}
			if order.Direction == sqlparser.DescScr {
				cmp.Operator = sqlparser.GreaterThanStr
			}

			val, err := vm.Eval(map[string]driver.Value{"lhs": lhs, "rhs": rhs}, cmp)
			if err != nil {
				return false
			}
			return reflect.ValueOf(val).IsValid() && !reflect.ValueOf(val).IsZero()
		})
	}

	columns = nil
	values = nil
	for _, record := range records {
		cols, vals := schema.Unbind(record)
		columns = append(columns, cols)
		values = append(values, vals)
	}
	return schema.NewInMemoryRows(columns, values), nil
}

type LimitTask struct {
	Input Task
	Limit *sqlparser.Limit
}

var _ Task = (*LimitTask)(nil)

func (t *LimitTask) Run(ctx context.Context) (driver.Value, error) {
	val, err := t.Input.Run(ctx)
	if err != nil {
		return nil, err
	}
	rows, ok := val.(driver.Rows)
	if !ok {
		return nil, NewErrUnsupportedType(val)
	}
	columns, values, err := schema.ReadAll(rows)
	if err != nil {
		return nil, err
	}

	offset := 0
	if t.Limit.Offset != nil {
		if val, err := vm.Eval(nil, t.Limit.Offset); err != nil {
			return nil, err
		} else if v := reflect.ValueOf(val); v.CanInt() {
			offset = int(v.Int())
		}
	}

	rowcount := len(columns)
	if t.Limit.Rowcount != nil {
		if val, err := vm.Eval(nil, t.Limit.Rowcount); err != nil {
			return nil, err
		} else if v := reflect.ValueOf(val); v.CanInt() {
			rowcount = int(v.Int())
		}
	}

	if offset >= rowcount {
		offset = rowcount - 1
	}

	if offset+rowcount > len(values) {
		rowcount = len(values) - offset
	}

	columns = columns[offset : offset+rowcount]
	values = values[offset : offset+rowcount]

	return schema.NewInMemoryRows(columns, values), nil
}
