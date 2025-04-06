package schema

import "database/sql/driver"

type InMemoryResult struct {
	lastInsertID int64
	rowsAffected int64
}

var _ driver.Result = (*InMemoryResult)(nil)

func NewInMemoryResult(lastInsertID, rowsAffected int64) *InMemoryResult {
	return &InMemoryResult{lastInsertID, rowsAffected}
}

func (r *InMemoryResult) LastInsertId() (int64, error) {
	return r.lastInsertID, nil
}

func (r *InMemoryResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}
