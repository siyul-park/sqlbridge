package engine

import (
	"context"
	"fmt"

	"github.com/xwb1989/sqlparser"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
)

type ValArgExpr struct {
	Value string
}

var _ Expr = (*ValArgExpr)(nil)

func (e *ValArgExpr) Eval(_ context.Context, _ schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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

func (e *ValArgExpr) String() string {
	return fmt.Sprintf("ValArg(%s)", e.Value)
}

type ColumnExpr struct {
	Value *sqlparser.ColName
}

var _ Expr = (*ColumnExpr)(nil)

func (e *ColumnExpr) Eval(_ context.Context, row schema.Row, _ map[string]*querypb.BindVariable) (Value, error) {
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

func (e *ColumnExpr) String() string {
	return fmt.Sprintf("Column(%s)", sqlparser.String(e.Value))
}

type TableExpr struct {
	Value sqlparser.TableName
}

var _ Expr = (*TableExpr)(nil)

func (e *TableExpr) Eval(_ context.Context, row schema.Row, _ map[string]*querypb.BindVariable) (Value, error) {
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

func (e *TableExpr) String() string {
	return fmt.Sprintf("Table(%s)", e.Value.Name)
}
