package schema

type Schema interface {
	Table(name string) (Table, bool)
}

type schema struct {
	tables map[string]Table
}

var _ Schema = (*schema)(nil)

func New(tables map[string]Table) Schema {
	if tables == nil {
		tables = make(map[string]Table)
	}
	return &schema{tables: tables}
}

func (s *schema) Table(name string) (Table, bool) {
	table, ok := s.tables[name]
	return table, ok
}
