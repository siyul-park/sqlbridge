package driver

import (
	"context"
	"database/sql/driver"

	"github.com/siyul-park/sqlbridge/plan"
	"github.com/siyul-park/sqlbridge/task"
)

type Connection struct {
	planner *plan.Planner
	builder *task.Builder
}

var _ driver.Conn = (*Connection)(nil)
var _ driver.Pinger = (*Connection)(nil)
var _ driver.Validator = (*Connection)(nil)
var _ driver.SessionResetter = (*Connection)(nil)
var _ driver.ExecerContext = (*Connection)(nil)
var _ driver.QueryerContext = (*Connection)(nil)
var _ driver.ConnPrepareContext = (*Connection)(nil)
var _ driver.ConnBeginTx = (*Connection)(nil)

func (c *Connection) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *Connection) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *Connection) Ping(_ context.Context) error {
	return nil
}

func (c *Connection) ResetSession(_ context.Context) error {
	return nil
}

func (c *Connection) IsValid() bool {
	return true
}

func (c *Connection) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	stmt, err := c.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	return stmt.(driver.StmtExecContext).ExecContext(ctx, args)
}

func (c *Connection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	stmt, err := c.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	return stmt.(driver.StmtQueryContext).QueryContext(ctx, args)
}

func (c *Connection) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return &Statement{planner: c.planner, builder: c.builder, query: query}, nil
	}
}

func (c *Connection) BeginTx(_ context.Context, _ driver.TxOptions) (driver.Tx, error) {
	return nil, driver.ErrSkip
}

func (c *Connection) Close() error {
	return nil
}
