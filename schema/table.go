package schema

import (
	"context"
)

type Table interface {
	Scan(ctx context.Context) (Rows, error)
}

type InMemoryTable struct {
	records []*Record
}

var _ Table = (*InMemoryTable)(nil)

func NewInMemoryTable(records []*Record) *InMemoryTable {
	return &InMemoryTable{records: records}
}

func (t *InMemoryTable) Scan(_ context.Context) (Rows, error) {
	return NewInMemoryRows(t.records), nil
}
