package driver

import (
	"database/sql/driver"

	"github.com/siyul-park/sqlbridge/vm"

	"github.com/siyul-park/sqlbridge/plan"
	"github.com/siyul-park/sqlbridge/schema"
	"github.com/siyul-park/sqlbridge/task"
)

type Driver struct {
	registry schema.Registry
	planner  *plan.Planner
	vm       *vm.VM
}

type Option func(*Driver)

var _ driver.Driver = (*Driver)(nil)
var _ driver.DriverContext = (*Driver)(nil)

func WithVM(vm *vm.VM) Option {
	return func(d *Driver) { d.vm = vm }
}

func WithPlanner(planner *plan.Planner) Option {
	return func(d *Driver) { d.planner = planner }
}

func WithRegistry(registry schema.Registry) Option {
	return func(d *Driver) { d.registry = registry }
}

func New(opts ...Option) *Driver {
	d := &Driver{
		vm:       vm.New(),
		registry: schema.NewInMemoryRegistry(nil),
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

func (d *Driver) Open(name string) (driver.Conn, error) {
	catalog, err := d.registry.Catalog(name)
	if err != nil {
		return nil, err
	}
	return &connection{
		planner: d.planner,
		builder: task.NewBuilder(task.WithVM(d.vm), task.WithCatalog(catalog)),
	}, nil
}

func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	return &connector{name: name, driver: d}, nil
}
