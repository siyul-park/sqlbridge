package schema

import (
	"context"

	"github.com/xwb1989/sqlparser/dependency/sqltypes"
)

type Table interface {
	Indexes(ctx context.Context) ([]Index, error)
	Scan(ctx context.Context, hint ...ScanHint) (Cursor, error)
}

type ScanHint struct {
	Index  string
	Ranges []Range
}

type Range struct {
	Min *sqltypes.Value
	Max *sqltypes.Value
}

type InMemoryTable struct {
	rows []Row
}

var _ Table = (*InMemoryTable)(nil)

func NewInMemoryTable(rows []Row) *InMemoryTable {
	return &InMemoryTable{rows: rows}
}

func (t *InMemoryTable) Indexes(_ context.Context) ([]Index, error) {
	return nil, nil
}

func (t *InMemoryTable) Scan(_ context.Context, _ ...ScanHint) (Cursor, error) {
	return NewInMemoryCursor(t.rows), nil
}
