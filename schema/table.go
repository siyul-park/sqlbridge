package schema

import (
	"context"
	"database/sql/driver"
)

type Table interface {
	Rows(ctx context.Context) (driver.Rows, error)
}

type inlineTable struct {
	columns [][]string
	values  [][]driver.Value
}

var _ Table = (*inlineTable)(nil)

func NewInlineTable(columns [][]string, rows [][]driver.Value) Table {
	return &inlineTable{columns: columns, values: rows}
}

func (t *inlineTable) Rows(_ context.Context) (driver.Rows, error) {
	return NewInlineRows(t.columns, t.values), nil
}
