package schema

import (
	"context"
	"database/sql/driver"
)

type Table interface {
	Scan(ctx context.Context) (driver.Rows, error)
}

type InMemoryTable struct {
	columns [][]string
	values  [][]driver.Value
}

var _ Table = (*InMemoryTable)(nil)

func NewInMemoryTable(columns [][]string, rows [][]driver.Value) *InMemoryTable {
	return &InMemoryTable{
		columns: columns,
		values:  rows,
	}
}

func (t *InMemoryTable) Scan(ctx context.Context) (driver.Rows, error) {
	return NewInMemoryRows(t.columns, t.values), nil
}
