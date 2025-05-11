package schema

import "github.com/pkg/errors"

type Catalog interface {
	Table(name string) (Table, error)
}

type CompositeCatalog struct {
	catalogs []Catalog
}

type InMemoryCatalog struct {
	tables map[string]Table
}

var ErrTableNotFound = errors.New("table not found")

var (
	_ Catalog = (*CompositeCatalog)(nil)
	_ Catalog = (*InMemoryCatalog)(nil)
)

func NewCompositeCatalog(catalogs ...Catalog) *CompositeCatalog {
	return &CompositeCatalog{catalogs: catalogs}
}

func NewInMemoryCatalog(tables map[string]Table) *InMemoryCatalog {
	if tables == nil {
		tables = make(map[string]Table)
	}
	return &InMemoryCatalog{tables: tables}
}

func (c *CompositeCatalog) Table(name string) (Table, error) {
	for _, catalog := range c.catalogs {
		table, err := catalog.Table(name)
		if err == nil {
			return table, nil
		}
		if !errors.Is(err, ErrTableNotFound) {
			return nil, err
		}
	}
	return nil, errors.WithStack(ErrTableNotFound)
}

func (c *InMemoryCatalog) Table(name string) (Table, error) {
	table, ok := c.tables[name]
	if !ok {
		return nil, errors.WithStack(ErrTableNotFound)
	}
	return table, nil
}
