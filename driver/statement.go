package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"slices"

	"github.com/siyul-park/sqlbridge/plan"
	"github.com/siyul-park/sqlbridge/schema"
	"github.com/siyul-park/sqlbridge/task"
	"github.com/xwb1989/sqlparser"
)

type Statement struct {
	builder *task.Builder
	plan    plan.Plan
	binds   map[string]struct{}
}

var _ driver.Stmt = (*Statement)(nil)
var _ driver.StmtExecContext = (*Statement)(nil)
var _ driver.StmtQueryContext = (*Statement)(nil)

func (s *Statement) NumInput() int {
	return len(s.binds)
}

func (s *Statement) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), s.named(args))
}

func (s *Statement) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), s.named(args))
}

func (s *Statement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	t, err := s.builder.Build(s.plan, args...)
	if err != nil {
		return nil, err
	}

	cursor, err := t.Run(ctx)
	if err != nil {
		return nil, err
	}

	records, err := schema.ReadAll(cursor)
	if err != nil {
		return nil, err
	}

	return &result{0, int64(len(records))}, nil
}

func (s *Statement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	t, err := s.builder.Build(s.plan, args...)
	if err != nil {
		return nil, err
	}

	cursor, err := t.Run(ctx)
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

func (s *Statement) Close() error {
	return nil
}

func (s *Statement) named(args []driver.Value) []driver.NamedValue {
	value := make([]driver.NamedValue, 0, len(args))
	for i, arg := range args {
		value = append(value, driver.NamedValue{Name: fmt.Sprintf("v%d", i), Value: arg})
	}
	return value
}
