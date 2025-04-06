package driver

import (
	"database/sql/driver"
)

type result struct {
	lastInsertID int64
	rowsAffected int64
}

var _ driver.Result = (*result)(nil)

func (r *result) LastInsertId() (int64, error) {
	return r.lastInsertID, nil
}

func (r *result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}
