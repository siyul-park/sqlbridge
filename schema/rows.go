package schema

import (
	"database/sql/driver"
	"github.com/xwb1989/sqlparser"
	"io"
	"strings"
)

type inlineRows struct {
	columns [][]string
	values  [][]driver.Value
}

var _ driver.Rows = (*inlineRows)(nil)

func ScanRows(rows driver.Rows) ([]map[*sqlparser.ColName]driver.Value, error) {
	var records []map[*sqlparser.ColName]driver.Value
	for {
		cols := rows.Columns()
		vals := make([]driver.Value, len(cols))
		if err := rows.Next(vals); err != nil {
			break
		}

		record := make(map[*sqlparser.ColName]driver.Value)
		for i, col := range cols {
			parts := strings.Split(col, ".")
			name := &sqlparser.ColName{Name: sqlparser.NewColIdent(parts[len(parts)-1])}
			if len(parts) > 1 {
				name.Qualifier = sqlparser.TableName{Qualifier: sqlparser.NewTableIdent(parts[0])}
			}
			record[name] = vals[i]
		}
		records = append(records, record)
	}
	return records, nil
}

func FormatRows(records []map[*sqlparser.ColName]driver.Value) driver.Rows {
	var columns [][]string
	var values [][]driver.Value

	for _, record := range records {
		var cols []string
		var vals []driver.Value
		for col, val := range record {
			cols = append(cols, sqlparser.String(col))
			vals = append(vals, val)
		}
		columns = append(columns, cols)
		values = append(values, vals)
	}

	return NewInlineRows(columns, values)
}

func NewInlineRows(columns [][]string, values [][]driver.Value) driver.Rows {
	return &inlineRows{
		columns: columns,
		values:  values,
	}
}

func (r *inlineRows) Columns() []string {
	if len(r.columns) == 0 {
		return nil
	}
	return r.columns[0]
}

func (r *inlineRows) Next(dest []driver.Value) error {
	if len(r.values) == 0 {
		return io.EOF
	}

	copy(dest, r.values[0])
	r.values = r.values[1:]
	r.columns = r.columns[1:]
	return nil
}

func (r *inlineRows) Close() error {
	r.values = nil
	r.columns = nil
	return nil
}
