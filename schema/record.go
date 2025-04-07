package schema

import (
	"database/sql/driver"
	"reflect"

	"github.com/xwb1989/sqlparser"
)

type Record struct {
	Keys    []Key
	Columns []*sqlparser.ColName
	Values  []driver.Value
}

type Metadata struct {
	Hidden bool
}

var GroupColumn = &sqlparser.ColName{
	Metadata: Metadata{Hidden: true},
	Name:     sqlparser.NewColIdent("__group__"),
}

func (r Record) Get(key *sqlparser.ColName) (driver.Value, bool) {
	for col, val := range r.Range() {
		if reflect.DeepEqual(col.Metadata, key.Metadata) && col.Name.Equal(key.Name) && (key.Qualifier.IsEmpty() || col.Qualifier == key.Qualifier) {
			return val, true
		}
	}
	return nil, false
}

func (r Record) Range() func(func(col *sqlparser.ColName, val driver.Value) bool) {
	return func(yield func(col *sqlparser.ColName, val driver.Value) bool) {
		for i, col := range r.Columns {
			if !yield(col, r.Values[i]) {
				return
			}
		}
	}
}

func (r Record) Equal(other Record) bool {
	for col, val1 := range r.Range() {
		val2, ok := other.Get(col)
		if !ok {
			return false
		}
		if !reflect.DeepEqual(val1, val2) {
			return false
		}
	}
	for col, val1 := range other.Range() {
		val2, ok := r.Get(col)
		if !ok {
			return false
		}
		if !reflect.DeepEqual(val1, val2) {
			return false
		}
	}
	return true
}
