package schema

import (
	"database/sql/driver"
	"io"
)

type InMemoryRows struct {
	columns [][]string
	values  [][]driver.Value
	offset  int
}

var _ driver.Rows = (*InMemoryRows)(nil)

func ReadAll(rows driver.Rows) ([][]string, [][]driver.Value, error) {
	columns := make([][]string, 0)
	values := make([][]driver.Value, 0)
	for {
		cols := rows.Columns()
		vals := make([]driver.Value, len(cols))
		if err := rows.Next(vals); err != nil {
			if err == io.EOF {
				break
			}
			_ = rows.Close()
			return nil, nil, err
		}
		columns = append(columns, cols)
		values = append(values, vals)
	}
	_ = rows.Close()
	return columns, values, nil
}

func NewInMemoryRows(columns [][]string, values [][]driver.Value) *InMemoryRows {
	if len(columns) == 0 {
		columns = nil
	}
	if len(values) == 0 {
		values = nil
	}
	return &InMemoryRows{
		columns: columns,
		values:  values,
	}
}

func (r *InMemoryRows) Columns() []string {
	if r.offset < len(r.columns) {
		columns := make([]string, len(r.columns[r.offset]))
		copy(columns, r.columns[r.offset])
		return columns
	}
	return nil
}

func (r *InMemoryRows) Close() error {
	return nil
}

func (r *InMemoryRows) Next(dest []driver.Value) error {
	if r.offset < len(r.values) {
		row := r.values[r.offset]
		if len(dest) != len(row) {
			return driver.ErrRemoveArgument
		}
		copy(dest, row)
		r.offset++
		return nil
	}
	return io.EOF
}
