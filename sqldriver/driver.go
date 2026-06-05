// Package sqldriver exposes the minivm-backed SQL engine as a database/sql
// driver. It compiles each statement to a minivm program (compile package) and
// runs it against a pluggable catalog (catalog package) through the runtime
// host-function bridge.
package sqldriver

import (
	"context"
	"database/sql/driver"

	"github.com/pkg/errors"
	"github.com/xwb1989/sqlparser"

	"github.com/siyul-park/sqlbridge/catalog"
	"github.com/siyul-park/sqlbridge/compile"
)

// Driver is a database/sql driver over a catalog registry.
type Driver struct {
	registry catalog.Registry
}

// Option configures a Driver.
type Option func(*Driver)

var (
	_ driver.Driver        = (*Driver)(nil)
	_ driver.DriverContext = (*Driver)(nil)
)

// WithRegistry sets the catalog registry the driver resolves databases from.
func WithRegistry(registry catalog.Registry) Option {
	return func(d *Driver) { d.registry = registry }
}

// New builds a Driver. By default it uses an empty in-memory registry.
func New(opts ...Option) *Driver {
	d := &Driver{registry: catalog.NewInMemoryRegistry(nil)}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

// Open resolves the catalog named name and returns a connection bound to it.
func (d *Driver) Open(name string) (driver.Conn, error) {
	cat, err := d.registry.Catalog(name)
	if err != nil {
		return nil, err
	}
	return &connection{compiler: compile.New(cat)}, nil
}

// OpenConnector returns a connector for name.
func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	return &connector{name: name, driver: d}, nil
}

// connector binds a driver to a catalog name.
type connector struct {
	name   string
	driver *Driver
}

var _ driver.Connector = (*connector)(nil)

func (c *connector) Connect(_ context.Context) (driver.Conn, error) {
	return c.driver.Open(c.name)
}

func (c *connector) Driver() driver.Driver { return c.driver }

// connection is a single logical connection: it compiles and runs statements
// against one catalog.
type connection struct {
	compiler *compile.Compiler
}

var (
	_ driver.Conn           = (*connection)(nil)
	_ driver.QueryerContext = (*connection)(nil)
)

func (c *connection) Prepare(query string) (driver.Stmt, error) {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	prog, err := c.compiler.Compile(stmt)
	if err != nil {
		return nil, err
	}
	return &statement{prog: prog}, nil
}

func (c *connection) Close() error { return nil }

func (c *connection) Begin() (driver.Tx, error) {
	return nil, errors.New("sqldriver: transactions are not supported")
}

// QueryContext compiles and runs query, returning its rows.
func (c *connection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	prog, err := c.compiler.Compile(stmt)
	if err != nil {
		return nil, err
	}
	return run(ctx, prog, namedBinds(args))
}

// statement is a prepared, compiled program.
type statement struct {
	prog *compile.Program
}

var (
	_ driver.Stmt             = (*statement)(nil)
	_ driver.StmtQueryContext = (*statement)(nil)
	_ driver.StmtExecContext  = (*statement)(nil)
)

func (s *statement) Close() error  { return nil }
func (s *statement) NumInput() int { return -1 }

func (s *statement) Exec(args []driver.Value) (driver.Result, error) {
	return runExec(context.Background(), s.prog, valueBinds(args))
}

func (s *statement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return runExec(ctx, s.prog, namedBinds(args))
}

func (s *statement) Query(args []driver.Value) (driver.Rows, error) {
	return run(context.Background(), s.prog, valueBinds(args))
}

func (s *statement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return run(ctx, s.prog, namedBinds(args))
}
