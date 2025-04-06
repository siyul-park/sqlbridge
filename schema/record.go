package schema

import (
	"database/sql/driver"

	"github.com/xwb1989/sqlparser"
)

type Record struct {
	IDs     []ID
	Columns []*sqlparser.ColName
	Values  []driver.Value
}

func (r *Record) Get(key *sqlparser.ColName) (driver.Value, bool) {
	for i, col := range r.Columns {
		if col.Name.Equal(key.Name) && (key.Qualifier.IsEmpty() || col.Qualifier == key.Qualifier) {
			return r.Values[i], true
		}
	}
	return nil, false
}

func (r *Record) Range() func(func(col *sqlparser.ColName, val driver.Value) bool) {
	return func(yield func(col *sqlparser.ColName, val driver.Value) bool) {
		for i, col := range r.Columns {
			if !yield(col, r.Values[i]) {
				return
			}
		}
	}
}

func (r *Record) Copy() *Record {
	c := &Record{}
	if len(r.IDs) > 0 {
		c.IDs = append(c.IDs, r.IDs...)
	}
	if len(r.Columns) > 0 {
		c.Columns = append(c.Columns, r.Columns...)
	}
	if len(r.Values) > 0 {
		c.Values = append(c.Values, r.Values...)
	}
	return c
}
