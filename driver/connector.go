package driver

import (
	"context"
	"database/sql/driver"
)

type connector struct {
	name   string
	driver *Driver
}

var _ driver.Connector = (*connector)(nil)

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.driver.Open(c.name)
	if err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		_ = conn.Close()
		return nil, ctx.Err()
	default:
		return conn, nil
	}
}

func (c *connector) Driver() driver.Driver {
	return c.driver
}
