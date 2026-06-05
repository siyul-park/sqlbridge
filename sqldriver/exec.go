package sqldriver

import (
	"context"
	"database/sql/driver"

	"github.com/pkg/errors"
	"github.com/siyul-park/minivm/interp"
	"github.com/xwb1989/sqlparser"

	"github.com/siyul-park/sqlbridge/compile"
	"github.com/siyul-park/sqlbridge/runtime"
)

var _ driver.ExecerContext = (*connection)(nil)

// ExecContext compiles and runs a write statement, returning the affected count.
func (c *connection) ExecContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	prog, err := c.compiler.Compile(stmt)
	if err != nil {
		return nil, err
	}
	return runExec(ctx, prog)
}

// runExec executes a write program and reports the number of affected rows.
func runExec(ctx context.Context, prog *compile.Program) (driver.Result, error) {
	vm := interp.New(prog.Program)
	defer vm.Close()

	sess := runtime.NewSession(runtime.NewResult(nil))
	if err := vm.Run(runtime.WithSession(ctx, sess)); err != nil {
		return nil, err
	}
	return result{affected: sess.Affected()}, nil
}

// result reports the rows affected by a write.
type result struct {
	affected int64
}

var _ driver.Result = result{}

func (r result) LastInsertId() (int64, error) {
	return 0, errors.New("sqldriver: LastInsertId is not supported")
}

func (r result) RowsAffected() (int64, error) {
	return r.affected, nil
}
