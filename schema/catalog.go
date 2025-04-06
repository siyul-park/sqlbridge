package schema

import (
	"fmt"
)

type Catalog interface {
	Table(name string) (Table, error)
}

type InMemoryCatalog struct {
	tables map[string]Table
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
		return nil, fmt.Errorf("table not fount: %v", name)
	}
	return table, nil
}
