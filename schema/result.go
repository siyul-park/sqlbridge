package schema

import "database/sql/driver"

type InMemoryResult struct {
	lastInsertID int64
	rowsAffected int64
}

var _ driver.Result = (*InMemoryResult)(nil)

func NewInMemoryResult(records []*Record) *InMemoryResult {
	var lastInsertID int64
	for i := len(records) - 1; i >= 0; i-- {
		if ids := records[i].IDs; len(ids) > 0 {
			lastInsertID = ids[len(ids)-1].Value
		}
	}
	rowsAffected := int64(len(records))
	return &InMemoryResult{lastInsertID, rowsAffected}
}

func (r *InMemoryResult) LastInsertId() (int64, error) {
	return r.lastInsertID, nil
}

func (r *InMemoryResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}
