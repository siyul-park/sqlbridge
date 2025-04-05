package schema

import (
	"database/sql/driver"
	"io"
)

type inlineRows struct {
	columns [][]string
	values  [][]driver.Value
}

var _ driver.Rows = (*inlineRows)(nil)

func NewInlineRows(columns [][]string, values [][]driver.Value) driver.Rows {
	return &inlineRows{
		columns: columns,
		values:  values,
	}
}

func (r *inlineRows) Columns() []string {
	if len(r.columns) == 0 {
		return nil
	}
	return r.columns[0]
}

func (r *inlineRows) Next(dest []driver.Value) error {
	if len(r.values) == 0 {
		return io.EOF
	}

	copy(dest, r.values[0])
	r.values = r.values[1:]
	r.columns = r.columns[1:]
	return nil
}

func (r *inlineRows) Close() error {
	r.values = nil
	r.columns = nil
	return nil
}
