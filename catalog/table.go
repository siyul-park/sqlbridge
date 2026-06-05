package catalog

import (
	"context"
	"sync"

	"github.com/xwb1989/sqlparser/dependency/sqltypes"
)

// Table is a readable relation. Backends implement it to expose scannable rows
// with optional index push-down via ScanHint.
type Table interface {
	Indexes(ctx context.Context) ([]Index, error)
	Scan(ctx context.Context, hint ...ScanHint) (Cursor, error)
}

// Writer is an optional capability for mutable tables. Write operators detect
// it via a type assertion; read-only backends simply omit it. Update is
// expressed as a delete of the old rows followed by an insert of the new ones.
type Writer interface {
	Insert(ctx context.Context, rows ...Row) (int64, error)
	Delete(ctx context.Context, rows ...Row) (int64, error)
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

var (
	_ Table  = (*InMemoryTable)(nil)
	_ Writer = (*InMemoryTable)(nil)
)

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

	return NewInMemoryCursor(append([]Row(nil), t.rows...)), nil
}

// Insert appends rows and returns the number inserted.
func (t *InMemoryTable) Insert(_ context.Context, rows ...Row) (int64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.rows = append(t.rows, rows...)
	return int64(len(rows)), nil
}

// Delete removes the first stored row equal to each given row and returns the
// number removed.
func (t *InMemoryTable) Delete(_ context.Context, rows ...Row) (int64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	var removed int64
	for _, target := range rows {
		for i, row := range t.rows {
			if rowEqual(row, target) {
				t.rows = append(t.rows[:i], t.rows[i+1:]...)
				removed++
				break
			}
		}
	}
	return removed, nil
}

// rowEqual reports whether two rows have identical columns and raw values.
func rowEqual(a, b Row) bool {
	if len(a.Columns) != len(b.Columns) || len(a.Values) != len(b.Values) {
		return false
	}
	for i := range a.Columns {
		if !a.Columns[i].Equal(b.Columns[i]) {
			return false
		}
	}
	for i := range a.Values {
		if a.Values[i].Type() != b.Values[i].Type() {
			return false
		}
		if string(a.Values[i].Raw()) != string(b.Values[i].Raw()) {
			return false
		}
	}
	return true
}
