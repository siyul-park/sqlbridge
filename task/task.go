package task

import (
	"context"
	"database/sql/driver"
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
	return nil, nil
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

	records, err := schema.ReadAll(cursor)
	if err != nil {
		return nil, err
	}

	if !t.Alias.IsEmpty() {
		for i, record := range records {
			columns := make([]*sqlparser.ColName, 0, len(record.Columns))
			for _, col := range record.Columns {
				columns = append(columns, &sqlparser.ColName{
					Metadata:  col.Metadata,
					Name:      col.Name,
					Qualifier: sqlparser.TableName{Name: t.Alias},
				})
			}
			record.Columns = columns
			records[i] = record
		}
	}
	return schema.NewInMemoryCursor(records), nil
}

type JoinTask struct {
	VM    *vm.VM
	Left  Task
	Right Task
	Join  string
	On    sqlparser.Expr
	Using []sqlparser.ColIdent
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

		val, err := t.VM.Eval(record, t.On)
		if err != nil {
			return false, err
		}
		if !reflect.ValueOf(val).IsValid() || reflect.ValueOf(val).IsZero() {
			return false, nil
		}
	}

	for _, using := range t.Using {
		lhs, _ := lhs.Get(&sqlparser.ColName{Name: using})
		rhs, _ := rhs.Get(&sqlparser.ColName{Name: using})

		record := schema.Record{
			Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("lhs")}, {Name: sqlparser.NewColIdent("rhs")}},
			Values:  []driver.Value{lhs, rhs},
		}

		val, err := t.VM.Eval(record, &sqlparser.ComparisonExpr{
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
	VM    *vm.VM
	Input Task
	Expr  sqlparser.Expr
}

var _ Task = (*FilterTask)(nil)

func (t *FilterTask) Run(ctx context.Context) (schema.Cursor, error) {
	cursor, err := t.Input.Run(ctx)
	if err != nil {
		return nil, err
	}

	records, err := schema.ReadAll(cursor)
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(records); i++ {
		val, err := t.VM.Eval(records[i], t.Expr)
		if err != nil {
			return nil, err
		}

		if !reflect.ValueOf(val).IsValid() || reflect.ValueOf(val).IsZero() {
			records = append(records[:i], records[i+1:]...)
			i--
		}
	}
	return schema.NewInMemoryCursor(records), nil
}

type ProjectTask struct {
	VM    *vm.VM
	Input Task
	Exprs sqlparser.SelectExprs
}

var _ Task = (*ProjectTask)(nil)

func (t *ProjectTask) Run(ctx context.Context) (schema.Cursor, error) {
	cursor, err := t.Input.Run(ctx)
	if err != nil {
		return nil, err
	}

	records, err := schema.ReadAll(cursor)
	if err != nil {
		return nil, err
	}

	for i, record := range records {
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

				val, err := t.VM.Eval(record, expr.Expr)
				if err != nil {
					return nil, err
				}

				columns = append(columns, col)
				values = append(values, val)
			default:
				return nil, driver.ErrSkip
			}
		}
		record.Columns = columns
		record.Values = values
		records[i] = record
	}
	return schema.NewInMemoryCursor(records), nil
}

type GroupTask struct {
	VM    *vm.VM
	Input Task
	Exprs sqlparser.GroupBy
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

			val, err := t.VM.Eval(record, expr)
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
			lhs, err := t.VM.Eval(records[i], order.Expr)
			if err != nil {
				return false
			}

			rhs, err := t.VM.Eval(records[j], order.Expr)
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

			record := schema.Record{
				Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("lhs")}, {Name: sqlparser.NewColIdent("rhs")}},
				Values:  []driver.Value{lhs, rhs},
			}

			val, err := t.VM.Eval(record, cmp)
			if err != nil {
				return false
			}
			return reflect.ValueOf(val).IsValid() && !reflect.ValueOf(val).IsZero()
		})
	}
	return schema.NewInMemoryCursor(records), nil
}

type LimitTask struct {
	VM    *vm.VM
	Input Task
	Exprs *sqlparser.Limit
}

var _ Task = (*LimitTask)(nil)

func (t *LimitTask) Run(ctx context.Context) (schema.Cursor, error) {
	cursor, err := t.Input.Run(ctx)
	if err != nil {
		return nil, err
	}

	records, err := schema.ReadAll(cursor)
	if err != nil {
		return nil, err
	}

	offset := 0
	if t.Exprs.Offset != nil {
		if val, err := t.VM.Eval(schema.Record{}, t.Exprs.Offset); err != nil {
			return nil, err
		} else if v := reflect.ValueOf(val); v.CanInt() {
			offset = int(v.Int())
		}
	}

	rowcount := len(records)
	if t.Exprs.Rowcount != nil {
		if val, err := t.VM.Eval(schema.Record{}, t.Exprs.Rowcount); err != nil {
			return nil, err
		} else if v := reflect.ValueOf(val); v.CanInt() {
			rowcount = int(v.Int())
		}
	}

	if offset >= rowcount {
		offset = rowcount - 1
	}
	if offset+rowcount > len(records) {
		rowcount = len(records) - offset
	}
	return schema.NewInMemoryCursor(records[offset : offset+rowcount]), nil
}
