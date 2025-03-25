package driver

import (
	"context"
	"database/sql/driver"
)

type Connector struct {
	name   string
	driver *Driver
}

var _ driver.Connector = (*Connector)(nil)

func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
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

func (c *Connector) Driver() driver.Driver {
	return c.driver
}
