package driver

import "database/sql/driver"

type transaction struct{}

var _ driver.Tx = (*transaction)(nil)

func (t *transaction) Commit() error {
	return nil
}

func (t *transaction) Rollback() error {
	return nil
}
