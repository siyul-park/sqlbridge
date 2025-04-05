package driver

import (
	"database/sql/driver"
	"fmt"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/siyul-park/sqlbridge/task"
	"github.com/siyul-park/sqlbridge/vm"
)

type Driver struct {
	tasks   *task.Registry
	schemas *schema.Registry
}

var _ driver.Driver = (*Driver)(nil)
var _ driver.DriverContext = (*Driver)(nil)

func New() *Driver {
	return &Driver{
		tasks:   task.DefaultRegistry,
		schemas: schema.NewRegistry(),
	}
}

func (d *Driver) AddBuilder(builder task.Builder) bool {
	return d.tasks.AddBuilder(builder)
}

func (d *Driver) RemoveBuilder(builder task.Builder) bool {
	return d.tasks.RemoveBuilder(builder)
}

func (d *Driver) AddSchema(name string, schema schema.Schema) bool {
	return d.schemas.AddSchema(name, schema)
}

func (d *Driver) RemoveSchema(name string) bool {
	return d.schemas.RemoveSchema(name)
}

func (d *Driver) Open(name string) (driver.Conn, error) {
	sc, ok := d.schemas.Schema(name)
	if !ok {
		return nil, fmt.Errorf("no schema with name %q exists", name)
	}
	return &Connection{vm: vm.New(vm.WithBuilder(d.tasks), vm.WithSchema(sc))}, nil
}

func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	return &Connector{name: name, driver: d}, nil
}
