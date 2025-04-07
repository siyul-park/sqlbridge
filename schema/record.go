package schema

import (
	"database/sql/driver"

	"github.com/xwb1989/sqlparser"
)

type Record struct {
	Keys    []Key
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
