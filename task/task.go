package task

import (
	"context"
	"database/sql/driver"
	"github.com/siyul-park/sqlbridge/schema"
	"github.com/siyul-park/sqlbridge/vm"
	"github.com/xwb1989/sqlparser"
	"reflect"
	"strings"
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
	Table schema.Table
}

var _ Task = (*NopTask)(nil)

func (t *ScanTask) Run(ctx context.Context) (driver.Value, error) {
	return t.Table.Scan(ctx)
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

		record := map[string]driver.Value{}
		for i := range columns {
			record[columns[i]] = values[i]
		}

		val, err := vm.Eval(record, t.On)
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
		record := map[string]driver.Value{}
		for j := range columns {
			record[columns[i][j]] = values[i][j]
		}

		val, err := vm.Eval(record, t.Expr)
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
		record := map[string]driver.Value{}
		for j := range columns {
			record[columns[i][j]] = values[i][j]
		}

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

				val, err := vm.Eval(record, expr.Expr)
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
