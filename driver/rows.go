package driver

import (
	"database/sql/driver"
	"io"
)

type rows struct {
	columns []string
	values  [][]driver.Value
	offset  int
}

var _ driver.Rows = (*rows)(nil)

func (r *rows) Columns() []string {
	return r.columns
}

func (r *rows) Next(dest []driver.Value) error {
	if r.offset >= len(r.values) {
		return io.EOF
	}
	row := r.values[r.offset]
	copy(dest, row[:len(dest)])
	r.offset++
	return nil
}

func (r *rows) Close() error {
	return nil
}
