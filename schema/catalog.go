package schema

import (
	"errors"
	"fmt"
)

type Catalog interface {
	Table(name string) (Table, error)
}

type InMemoryCatalog struct {
	tables map[string]Table
}

var ErrNotFound = errors.New("not found")

func NewErrNotFound(key any) error {
	return fmt.Errorf("%w: %v", ErrNotFound, key)
}

func NewInMemoryCatalog(tables map[string]Table) *InMemoryCatalog {
	if tables == nil {
		tables = make(map[string]Table)
	}
	return &InMemoryCatalog{tables: tables}
}

func (c *InMemoryCatalog) Table(name string) (Table, error) {
	table, ok := c.tables[name]
	if !ok {
		return nil, NewErrNotFound(name)
	}
	return table, nil
}
