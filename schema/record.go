package schema

import (
	"database/sql/driver"
	"errors"
	"io"
	"strings"

	"github.com/xwb1989/sqlparser"
)

type Record struct {
	IDs     []ID
	Columns []*sqlparser.ColName
	Values  []driver.Value
}

func ReadAll(rows Rows) ([]*Record, error) {
	var records []*Record
	for {
		record := &Record{IDs: rows.IDs()}

		for _, col := range rows.Columns() {
			tokens := strings.Split(col, ".")
			column := &sqlparser.ColName{}
			column.Name = sqlparser.NewColIdent(tokens[len(tokens)-1])
			if len(tokens) > 1 {
				column.Qualifier = sqlparser.TableName{Name: sqlparser.NewTableIdent(tokens[0])}
			}
			record.Columns = append(record.Columns, column)
		}

		record.Values = make([]driver.Value, len(record.Columns))
		if err := rows.Next(record.Values); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		records = append(records, record)
	}
	return records, nil
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
