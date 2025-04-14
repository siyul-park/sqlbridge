package schema

import (
	"context"
)

type Table interface {
	Scan(ctx context.Context) (Cursor, error)
}

type InMemoryTable struct {
	rows []Row
}

var _ Table = (*InMemoryTable)(nil)

func NewInMemoryTable(rows []Row) *InMemoryTable {
	return &InMemoryTable{rows: rows}
}

func (t *InMemoryTable) Scan(_ context.Context) (Cursor, error) {
	return NewInMemoryCursor(t.rows), nil
}
