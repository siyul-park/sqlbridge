package task

import (
	"context"
	"database/sql/driver"
	"io"
	"reflect"
	"sort"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/siyul-park/sqlbridge/vm"
	"github.com/xwb1989/sqlparser"
)

type Task interface {
	Run(ctx context.Context) (schema.Cursor, error)
}

type NopTask struct{}

var _ Task = (*NopTask)(nil)

func (t *NopTask) Run(_ context.Context) (schema.Cursor, error) {
	return schema.NewInMemoryCursor(nil), nil
}

type ScanTask struct {
	Catalog schema.Catalog
	Table   sqlparser.TableName
}

var _ Task = (*NopTask)(nil)

func (t *ScanTask) Run(ctx context.Context) (schema.Cursor, error) {
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

func (t *AliasTask) Run(ctx context.Context) (schema.Cursor, error) {
	cursor, err := t.Input.Run(ctx)
	if err != nil {
		return nil, err
	}

	return schema.NewMappedCursor(cursor, func(record schema.Record) (schema.Record, error) {
		columns := make([]*sqlparser.ColName, 0, len(record.Columns))
		for _, col := range record.Columns {
			columns = append(columns, &sqlparser.ColName{
				Metadata:  col.Metadata,
				Name:      col.Name,
				Qualifier: sqlparser.TableName{Name: t.Alias},
			})
		}
		record.Columns = columns
		return record, nil
	}), nil
}

type JoinTask struct {
	VM    *vm.VM
	Left  Task
	Right Task
	Join  string
	On    sqlparser.Expr
	Using []sqlparser.ColIdent
	Args  []driver.NamedValue
}

var _ Task = (*JoinTask)(nil)

func (t *JoinTask) Run(ctx context.Context) (schema.Cursor, error) {
	lcsr, err := t.Left.Run(ctx)
	if err != nil {
		return nil, err
	}
	rcsr, err := t.Right.Run(ctx)
	if err != nil {
		return nil, err
	}

	left, err := schema.ReadAll(lcsr)
	if err != nil {
		return nil, err
	}
	right, err := schema.ReadAll(rcsr)
	if err != nil {
		return nil, err
	}

	var joined []schema.Record
	switch t.Join {
	case sqlparser.JoinStr:
		for _, lhs := range left {
			for _, rhs := range right {
				ok, err := t.on(lhs, rhs)
				if err != nil {
					return nil, err
				}
				if ok {
					joined = append(joined, schema.Record{
						Keys:    append(lhs.Keys, rhs.Keys...),
						Columns: append(lhs.Columns, rhs.Columns...),
						Values:  append(lhs.Values, rhs.Values...),
					})
				}
			}
		}
	case sqlparser.LeftJoinStr, sqlparser.RightJoinStr:
		if t.Join == sqlparser.RightJoinStr {
			right, left = left, right
		}

		var columns []*sqlparser.ColName
		visits := map[string]struct{}{}
		for _, row := range right {
			for _, col := range row.Columns {
				key := sqlparser.String(col)
				if _, ok := visits[key]; ok {
					columns = append(columns, col)
					visits[key] = struct{}{}
				}
			}
		}

		for _, lhs := range left {
			matched := false
			for _, rhs := range right {
				ok, err := t.on(lhs, rhs)
				if err != nil {
					return nil, err
				}
				if ok {
					joined = append(joined, schema.Record{
						Keys:    append(lhs.Keys, rhs.Keys...),
						Columns: append(lhs.Columns, rhs.Columns...),
						Values:  append(lhs.Values, rhs.Values...),
					})
					matched = true
				}
			}
			if !matched {
				values := make([]driver.Value, len(columns))
				joined = append(joined, schema.Record{
					Keys:    lhs.Keys,
					Columns: append(lhs.Columns, columns...),
					Values:  append(lhs.Values, values...),
				})
			}
		}
	default:
		return nil, driver.ErrSkip
	}
	return schema.NewInMemoryCursor(joined), nil
}

func (t *JoinTask) on(lhs, rhs schema.Record) (bool, error) {
	if t.On != nil {
		record := schema.Record{
			Columns: append(lhs.Columns, rhs.Columns...),
			Values:  append(lhs.Values, rhs.Values...),
		}

		val, err := t.VM.Eval(t.On, record, t.Args...)
		if err != nil {
			return false, err
		}
		if !t.VM.Bool(val) {
			return false, nil
		}
	}

	for _, using := range t.Using {
		lhs, _ := lhs.Get(&sqlparser.ColName{Name: using})
		rhs, _ := rhs.Get(&sqlparser.ColName{Name: using})
		if !t.VM.Equal(lhs, rhs) {
			return false, nil
		}
	}
	return true, nil
}

type FilterTask struct {
	VM    *vm.VM
	Input Task
	Expr  sqlparser.Expr
	Args  []driver.NamedValue
}

var _ Task = (*FilterTask)(nil)

func (t *FilterTask) Run(ctx context.Context) (schema.Cursor, error) {
	cursor, err := t.Input.Run(ctx)
	if err != nil {
		return nil, err
	}

	return schema.NewMappedCursor(cursor, func(record schema.Record) (schema.Record, error) {
		val, err := t.VM.Eval(t.Expr, record, t.Args...)
		if err != nil {
			return schema.Record{}, err
		}
		if !t.VM.Bool(val) {
			return schema.Record{}, nil
		}
		return record, nil
	}), nil
}

type ProjectTask struct {
	VM    *vm.VM
	Input Task
	Exprs sqlparser.SelectExprs
	Args  []driver.NamedValue
}

var _ Task = (*ProjectTask)(nil)

func (t *ProjectTask) Run(ctx context.Context) (schema.Cursor, error) {
	cursor, err := t.Input.Run(ctx)
	if err != nil {
		return nil, err
	}

	return schema.NewMappedCursor(cursor, func(record schema.Record) (schema.Record, error) {
		var columns []*sqlparser.ColName
		var values []driver.Value
		for _, expr := range t.Exprs {
			switch expr := expr.(type) {
			case *sqlparser.StarExpr:
				for i := 0; i < len(record.Columns); i++ {
					meta, _ := record.Columns[i].Metadata.(schema.Metadata)

					col := &sqlparser.ColName{Name: record.Columns[i].Name}
					val := record.Values[i]

					if meta.Hidden && (!expr.TableName.IsEmpty() && record.Columns[i].Qualifier != expr.TableName) {
						continue
					}

					columns = append(columns, col)
					values = append(values, val)
				}
			case *sqlparser.AliasedExpr:
				col := &sqlparser.ColName{Name: sqlparser.NewColIdent(sqlparser.String(expr.Expr))}
				if !expr.As.IsEmpty() {
					col.Name = expr.As
				}

				val, err := t.VM.Eval(expr.Expr, record, t.Args...)
				if err != nil {
					return schema.Record{}, err
				}

				columns = append(columns, col)
				values = append(values, val)
			default:
				return schema.Record{}, driver.ErrSkip
			}
		}
		record.Columns = columns
		record.Values = values
		return record, nil
	}), nil
}

type GroupTask struct {
	VM    *vm.VM
	Input Task
	Exprs sqlparser.GroupBy
	Args  []driver.NamedValue
}

var _ Task = (*GroupTask)(nil)

func (t *GroupTask) Run(ctx context.Context) (schema.Cursor, error) {
	cursor, err := t.Input.Run(ctx)
	if err != nil {
		return nil, err
	}

	records, err := schema.ReadAll(cursor)
	if err != nil {
		return nil, err
	}

	groups := map[*schema.Record][]schema.Record{}
	for _, record := range records {
		key := schema.Record{Keys: record.Keys}
		for _, expr := range t.Exprs {
			col, ok := expr.(*sqlparser.ColName)
			if !ok {
				return nil, driver.ErrSkip
			}

			val, err := t.VM.Eval(expr, record, t.Args...)
			if err != nil {
				return nil, err
			}

			key.Columns = append(key.Columns, col)
			key.Values = append(key.Values, val)
		}

		ok := false
		for k := range groups {
			if k.Equal(key) {
				k.Keys = append(k.Keys, record.Keys...)
				groups[k] = append(groups[k], record)
				ok = true
				break
			}
		}
		if !ok {
			groups[&key] = []schema.Record{record}
		}
	}

	var grouped []schema.Record
	for k, group := range groups {
		grouped = append(grouped, schema.Record{
			Keys:    k.Keys,
			Columns: append(k.Columns, schema.GroupColumn),
			Values:  append(k.Values, group),
		})
	}
	return schema.NewInMemoryCursor(grouped), nil
}

type OrderTask struct {
	VM    *vm.VM
	Input Task
	Exprs sqlparser.OrderBy
	Args  []driver.NamedValue
}

var _ Task = (*OrderTask)(nil)

func (t *OrderTask) Run(ctx context.Context) (schema.Cursor, error) {
	cursor, err := t.Input.Run(ctx)
	if err != nil {
		return nil, err
	}

	records, err := schema.ReadAll(cursor)
	if err != nil {
		return nil, err
	}

	for _, order := range t.Exprs {
		sort.SliceStable(records, func(i, j int) bool {
			lhs, err := t.VM.Eval(order.Expr, records[i], t.Args...)
			if err != nil {
				return false
			}

			rhs, err := t.VM.Eval(order.Expr, records[j], t.Args...)
			if err != nil {
				return false
			}

			cmp := t.VM.Compare(lhs, rhs)
			if order.Direction == sqlparser.DescScr {
				cmp *= -1
			}
			return cmp < 0
		})
	}
	return schema.NewInMemoryCursor(records), nil
}

type LimitTask struct {
	VM    *vm.VM
	Input Task
	Exprs *sqlparser.Limit
	Args  []driver.NamedValue
}

var _ Task = (*LimitTask)(nil)

func (t *LimitTask) Run(ctx context.Context) (schema.Cursor, error) {
	cursor, err := t.Input.Run(ctx)
	if err != nil {
		return nil, err
	}

	offset := 0
	if t.Exprs.Offset != nil {
		if val, err := t.VM.Eval(t.Exprs.Offset, schema.Record{}, t.Args...); err != nil {
			return nil, err
		} else if v := reflect.ValueOf(val); v.CanInt() {
			offset = int(v.Int())
		}
	}

	rowcount := -1
	if t.Exprs.Rowcount != nil {
		if val, err := t.VM.Eval(t.Exprs.Rowcount, schema.Record{}, t.Args...); err != nil {
			return nil, err
		} else if v := reflect.ValueOf(val); v.CanInt() {
			rowcount = int(v.Int())
		}
	}

	return schema.NewMappedCursor(cursor, func(record schema.Record) (schema.Record, error) {
		if offset > 0 {
			offset--
			return schema.Record{}, nil
		}
		if rowcount == 0 {
			return schema.Record{}, io.EOF
		}
		rowcount--
		return record, nil
	}), nil
}
