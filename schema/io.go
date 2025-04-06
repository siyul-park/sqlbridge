package schema

import (
	"database/sql/driver"
	"io"
	"sort"
)

func ReadAll(rows driver.Rows) ([][]string, [][]driver.Value, error) {
	columns := make([][]string, 0)
	values := make([][]driver.Value, 0)
	for {
		cols := rows.Columns()
		vals := make([]driver.Value, len(cols))
		if err := rows.Next(vals); err != nil {
			if err == io.EOF {
				break
			}
			_ = rows.Close()
			return nil, nil, err
		}
		columns = append(columns, cols)
		values = append(values, vals)
	}
	_ = rows.Close()
	return columns, values, nil
}

func Bind(columns []string, values []driver.Value) map[string]driver.Value {
	record := make(map[string]driver.Value, len(columns))
	for i, column := range columns {
		record[column] = values[i]
	}
	return record
}

func Unbind(record map[string]driver.Value) ([]string, []driver.Value) {
	columns := make([]string, 0, len(record))
	for column := range record {
		columns = append(columns, column)
	}
	sort.Strings(columns)

	values := make([]driver.Value, 0, len(columns))
	for _, column := range columns {
		values = append(values, record[column])
	}
	return columns, values
}
