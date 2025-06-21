package schema

import (
	"context"
	"sync"

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
	indexes []Index
	rows    []Row
	mu      sync.RWMutex
}

var _ Table = (*InMemoryTable)(nil)

func NewInMemoryTable(rows []Row) *InMemoryTable {
	return &InMemoryTable{rows: rows}
}

func (t *InMemoryTable) Indexes(_ context.Context) ([]Index, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return append([]Index(nil), t.indexes...), nil
}

func (t *InMemoryTable) SetIndex(_ context.Context, index Index) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.indexes = append(t.indexes, index)
	return nil
}

func (t *InMemoryTable) Scan(_ context.Context, _ ...ScanHint) (Cursor, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return NewInMemoryCursor(t.rows), nil
}
