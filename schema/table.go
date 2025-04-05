package schema

import (
	"context"
	"database/sql/driver"
	"github.com/xwb1989/sqlparser"
)

type Table interface {
	Queryer
}

type inlineTable struct {
	columns [][]string
	values  [][]driver.Value
}

var _ Table = (*inlineTable)(nil)

func NewInlineTable(columns [][]string, rows [][]driver.Value) Table {
	return &inlineTable{columns: columns, values: rows}
}

func (t *inlineTable) Query(_ context.Context, _ sqlparser.SQLNode) (driver.Rows, error) {
	return NewInlineRows(t.columns, t.values), nil
}
