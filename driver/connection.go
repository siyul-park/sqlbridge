package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"

	"github.com/siyul-park/sqlbridge/plan"
	"github.com/xwb1989/sqlparser"
)

type connection struct {
	planner *plan.Planner
}

var _ driver.Conn = (*connection)(nil)
var _ driver.Pinger = (*connection)(nil)
var _ driver.Validator = (*connection)(nil)
var _ driver.SessionResetter = (*connection)(nil)
var _ driver.ExecerContext = (*connection)(nil)
var _ driver.QueryerContext = (*connection)(nil)
var _ driver.ConnPrepareContext = (*connection)(nil)
var _ driver.ConnBeginTx = (*connection)(nil)

func (c *connection) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *connection) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *connection) Ping(_ context.Context) error {
	return nil
}

func (c *connection) ResetSession(_ context.Context) error {
	return nil
}

func (c *connection) IsValid() bool {
	return true
}

func (c *connection) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	stmt, err := c.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	return stmt.(driver.StmtExecContext).ExecContext(ctx, args)
}

func (c *connection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	stmt, err := c.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	return stmt.(driver.StmtQueryContext).QueryContext(ctx, args)
}

func (c *connection) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		var idx int
		var buf strings.Builder
		for i := 0; i < len(query); i++ {
			if query[i] == '?' {
				idx++
				buf.WriteString(fmt.Sprintf(":v%d", idx))
			} else {
				buf.WriteByte(query[i])
			}
		}

		stmt, err := sqlparser.Parse(buf.String())
		if err != nil {
			return nil, err
		}

		p, err := c.planner.Plan(stmt)
		if err != nil {
			return nil, err
		}

		binds := sqlparser.GetBindvars(stmt)
		return &statement{plan: p, binds: binds}, nil
	}
}

func (c *connection) BeginTx(_ context.Context, _ driver.TxOptions) (driver.Tx, error) {
	return nil, driver.ErrSkip
}

func (c *connection) Close() error {
	return nil
}
