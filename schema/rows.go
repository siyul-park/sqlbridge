package schema

import (
	"database/sql/driver"
	"io"
	"sort"
)

type rows struct {
	records []map[string]driver.Value
	columns []string
}

var _ driver.Rows = (*rows)(nil)

func NewRows(records []map[string]driver.Value) driver.Rows {
	var columns []string
	if len(records) > 0 {
		for col := range records[0] {
			columns = append(columns, col)
		}
		sort.Strings(columns)
	}

	return &rows{records: records, columns: columns}
}

func (r *rows) Columns() []string {
	return r.columns
}

func (r *rows) Next(dest []driver.Value) error {
	if len(r.records) == 0 {
		return io.EOF
	}

	record := r.records[0]
	r.records = r.records[1:]

	for i := 0; i < len(r.columns) && i < len(dest); i++ {
		if val, ok := record[r.columns[i]]; ok {
			dest[i] = val
		} else {
			dest[i] = nil
		}
	}

	r.columns = nil
	if len(r.records) > 0 {
		for col := range r.records[0] {
			r.columns = append(r.columns, col)
		}
		sort.Strings(r.columns)
	}
	return nil
}

func (r *rows) Close() error {
	r.records = nil
	r.columns = nil
	return nil
}
