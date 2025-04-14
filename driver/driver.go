package driver

import (
	"database/sql/driver"

	"github.com/siyul-park/sqlbridge/eval"
	"github.com/siyul-park/sqlbridge/plan"
	"github.com/siyul-park/sqlbridge/schema"
)

type Driver struct {
	registry   schema.Registry
	dispatcher *eval.Dispatcher
}

type Option func(*Driver)

var _ driver.Driver = (*Driver)(nil)
var _ driver.DriverContext = (*Driver)(nil)

func WithRegistry(registry schema.Registry) Option {
	return func(d *Driver) { d.registry = registry }
}

func WithDispatcher(dispatcher *eval.Dispatcher) Option {
	return func(d *Driver) { d.dispatcher = dispatcher }
}

func New(opts ...Option) *Driver {
	d := &Driver{
		registry:   schema.NewInMemoryRegistry(nil),
		dispatcher: eval.NewDispatcher(eval.WithBuiltIn()),
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
	return &connection{planner: plan.NewPlanner(catalog, d.dispatcher)}, nil
}

func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	return &connector{name: name, driver: d}, nil
}
