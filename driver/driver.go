package driver

import (
	"database/sql/driver"

	"github.com/siyul-park/sqlbridge/plan"
	"github.com/siyul-park/sqlbridge/schema"
	"github.com/siyul-park/sqlbridge/task"
)

type Driver struct {
	registry *schema.Registry
}

var _ driver.Driver = (*Driver)(nil)
var _ driver.DriverContext = (*Driver)(nil)

func New(registry *schema.Registry) *Driver {
	if registry == nil {
		registry = schema.NewRegistry()
	}
	return &Driver{registry: registry}
}

func (d *Driver) Open(name string) (driver.Conn, error) {
	catalog, err := d.registry.Catalog(name)
	if err != nil {
		return nil, err
	}
	return &Connection{planner: plan.NewPlanner(), builder: task.NewBuilder(catalog)}, nil
}

func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	return &Connector{name: name, driver: d}, nil
}
