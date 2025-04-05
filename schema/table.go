package schema

import (
	"context"
	"database/sql/driver"
)

type Table interface {
	Rows(ctx context.Context) (driver.Rows, error)
}

type table struct {
	records []map[string]driver.Value
}

var _ Table = (*table)(nil)

func NewTable(records []map[string]driver.Value) Table {
	if records == nil {
		records = make([]map[string]driver.Value, 0)
	}
	return &table{records: records}
}

func (t *table) Rows(_ context.Context) (driver.Rows, error) {
	return NewRows(t.records), nil
}
