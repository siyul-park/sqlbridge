package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"slices"

	"github.com/siyul-park/sqlbridge/plan"
	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
)

type statement struct {
	plan  plan.Plan
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
	binds, err := s.bind(args)
	if err != nil {
		return nil, err
	}

	cursor, err := s.plan.Run(ctx, binds)
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
	binds, err := s.bind(args)
	if err != nil {
		return nil, err
	}

	cursor, err := s.plan.Run(ctx, binds)
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
			idx[col.Name.String()] = i
			if !slices.Contains(columns, col.Name.String()) {
				columns = append(columns, col.Name.String())
			}
		}

		var vals []driver.Value
		for _, col := range columns {
			if i, ok := idx[col]; !ok {
				vals = append(vals, nil)
			} else {
				val, err := schema.Unmarshal(row.Values[i])
				if err != nil {
					return nil, err
				}
				vals = append(vals, val)
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

func (s *statement) bind(args []driver.NamedValue) (map[string]*querypb.BindVariable, error) {
	binds := make(map[string]any, len(args))
	for _, arg := range args {
		binds[arg.Name] = arg.Value
	}
	return sqltypes.BuildBindVariables(binds)
}
