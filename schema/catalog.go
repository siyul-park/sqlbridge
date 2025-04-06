package schema

import "fmt"

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
	table, exists := c.tables[name]
	if !exists {
		return nil, fmt.Errorf("table '%s' not found in InMemoryCatalog. Ensure that the table name is correct or check if it has been registered properly", name)
	}
	return table, nil
}
