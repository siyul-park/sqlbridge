package eval

import (
	"context"
	"fmt"

	"github.com/xwb1989/sqlparser"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
)

type ValArg struct {
	Value string
}

var _ Expr = (*ValArg)(nil)

func (e *ValArg) Eval(_ context.Context, _ schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	val, ok := binds[e.Value]
	if !ok {
		return nil, nil
	}
	switch val.Type {
	case querypb.Type_TUPLE:
		vals := make([]Value, 0, len(val.Values))
		for _, v := range val.Values {
			val, err := FromSQL(sqltypes.MakeTrusted(v.Type, v.Value))
			if err != nil {
				return nil, err
			}
			vals = append(vals, val)
		}
		return NewTuple(vals), nil
	default:
		return FromSQL(sqltypes.MakeTrusted(val.Type, val.Value))
	}
}

func (e *ValArg) String() string {
	return fmt.Sprintf("ValArg(%s)", e.Value)
}

type Column struct {
	Value *sqlparser.ColName
}

var _ Expr = (*Column)(nil)

func (e *Column) Eval(_ context.Context, row schema.Row, _ map[string]*querypb.BindVariable) (Value, error) {
	var vals []Value
	for i, col := range row.Columns {
		if (e.Value.Qualifier.IsEmpty() && col.Name.Equal(e.Value.Name)) || (!e.Value.Qualifier.IsEmpty() && col.Equal(e.Value)) {
			val, err := FromSQL(row.Values[i])
			if err != nil {
				return nil, err
			}
			vals = append(vals, val)
		}
	}
	return NewTuple(vals), nil
}

func (e *Column) String() string {
	return fmt.Sprintf("Column(%s)", sqlparser.String(e.Value))
}

type Table struct {
	Value sqlparser.TableName
}

var _ Expr = (*Table)(nil)

func (e *Table) Eval(_ context.Context, row schema.Row, _ map[string]*querypb.BindVariable) (Value, error) {
	var vals []Value
	for i, col := range row.Columns {
		if !e.Value.IsEmpty() && col.Qualifier != e.Value {
			continue
		}
		val, err := FromSQL(row.Values[i])
		if err != nil {
			return nil, err
		}
		vals = append(vals, val)
	}
	return NewTuple(vals), nil
}

func (e *Table) String() string {
	return fmt.Sprintf("Table(%s)", e.Value.Name)
}
