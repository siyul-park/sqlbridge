package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"slices"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/siyul-park/sqlbridge/task"
	"github.com/xwb1989/sqlparser"
)

type statement struct {
	task  task.Task
	binds map[string]struct{}
}

var _ driver.Stmt = (*statement)(nil)
var _ driver.StmtExecContext = (*statement)(nil)
var _ driver.StmtQueryContext = (*statement)(nil)

func (s *statement) NumInput() int {
	return len(s.binds)
}

func (s *statement) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), s.named(args))
}

func (s *statement) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), s.named(args))
}

func (s *statement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	cursor, err := s.task.Run(ctx, args...)
	if err != nil {
		return nil, err
	}

	records, err := schema.ReadAll(cursor)
	if err != nil {
		return nil, err
	}

	return &result{0, int64(len(records))}, nil
}

func (s *statement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	cursor, err := s.task.Run(ctx, args...)
	if err != nil {
		return nil, err
	}

	records, err := schema.ReadAll(cursor)
	if err != nil {
		return nil, err
	}

	var columns []string
	var values [][]driver.Value
	for _, row := range records {
		idx := map[string]int{}
		for i, col := range row.Columns {
			idx[sqlparser.String(col)] = i
			if !slices.Contains(columns, sqlparser.String(col)) {
				columns = append(columns, sqlparser.String(col))
			}
		}

		var vals []driver.Value
		for _, col := range columns {
			if i, ok := idx[col]; !ok {
				vals = append(vals, nil)
			} else {
				vals = append(vals, row.Values[i])
			}
		}
		values = append(values, vals)
	}
	return &rows{columns: columns, values: values}, nil
}

func (s *statement) Close() error {
	return nil
}

func (s *statement) named(args []driver.Value) []driver.NamedValue {
	value := make([]driver.NamedValue, 0, len(args))
	for i, arg := range args {
		value = append(value, driver.NamedValue{Name: fmt.Sprintf("v%d", i), Value: arg})
	}
	return value
}
