package schema

import (
	"context"
)

type Table interface {
	Scan(ctx context.Context) (Cursor, error)
}

type InMemoryTable struct {
	records []*Record
}

var _ Table = (*InMemoryTable)(nil)

func NewInMemoryTable(records []*Record) *InMemoryTable {
	return &InMemoryTable{records: records}
}

func (t *InMemoryTable) Scan(_ context.Context) (Cursor, error) {
	return NewInMemoryCursor(t.records), nil
}
