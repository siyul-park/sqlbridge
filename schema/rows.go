package schema

import (
	"database/sql/driver"
	"io"
	"slices"

	"github.com/xwb1989/sqlparser"
)

type Rows interface {
	driver.Rows
	IDs() []ID
}

type InMemoryRows struct {
	columns []string
	ids     [][]ID
	values  [][]driver.Value
	offset  int
}

var _ Rows = (*InMemoryRows)(nil)

func NewInMemoryRows(records []*Record) *InMemoryRows {
	var columns []string
	var ids [][]ID
	var values [][]driver.Value
	for _, row := range records {
		idx := map[string]int{}
		for i, col := range row.Columns {
			idx[sqlparser.String(col)] = i
			if !slices.Contains(columns, sqlparser.String(col)) {
				columns = append(columns, sqlparser.String(col))
			}
		}

		ids = append(ids, row.IDs)

		var vals []driver.Value
		for _, col := range columns {
			if i, ok := idx[col]; !ok {
				vals = append(vals, nil)
			} else {
				vals = append(vals, row.Values[i])
			}
		}
		values = append(values, vals)
	}

	return &InMemoryRows{
		columns: columns,
		ids:     ids,
		values:  values,
	}
}

func (r *InMemoryRows) Columns() []string {
	return r.columns
}

func (r *InMemoryRows) IDs() []ID {
	if r.offset >= len(r.ids) {
		return nil
	}
	return r.ids[r.offset]
}

func (r *InMemoryRows) Next(dest []driver.Value) error {
	if r.offset >= len(r.values) {
		return io.EOF
	}
	row := r.values[r.offset]
	copy(dest, row[:len(dest)])
	r.offset++
	return nil
}

func (r *InMemoryRows) Close() error {
	return nil
}
