package sqldriver

import (
	"context"
	"database/sql/driver"
	"io"

	"github.com/siyul-park/minivm/interp"

	"github.com/siyul-park/sqlbridge/catalog"
	"github.com/siyul-park/sqlbridge/compile"
	"github.com/siyul-park/sqlbridge/runtime"
	"github.com/siyul-park/sqlbridge/value"
)

// run executes a compiled program and returns its rows.
func run(ctx context.Context, prog *compile.Program, binds map[string]value.Value) (driver.Rows, error) {
	vm := interp.New(prog.Program)
	defer vm.Close()

	res := runtime.NewResult(prog.Columns)
	sess := runtime.NewSession(res)
	sess.SetBinds(binds)
	if err := vm.Run(runtime.WithSession(ctx, sess)); err != nil {
		return nil, err
	}

	out := res.Rows()
	return &rows{columns: columnsOf(out), rows: out}, nil
}

// columnsOf resolves result column names from the materialized rows. Each row
// carries its own column names, which also covers SELECT *.
func columnsOf(rows []catalog.Row) []string {
	if len(rows) == 0 {
		return nil
	}
	names := make([]string, len(rows[0].Columns))
	for i, col := range rows[0].Columns {
		names[i] = col.Name.String()
	}
	return names
}

// rows iterates the materialized result set.
type rows struct {
	columns []string
	rows    []catalog.Row
	idx     int
}

var _ driver.Rows = (*rows)(nil)

func (r *rows) Columns() []string { return r.columns }

func (r *rows) Close() error { return nil }

func (r *rows) Next(dest []driver.Value) error {
	if r.idx >= len(r.rows) {
		return io.EOF
	}
	row := r.rows[r.idx]
	r.idx++

	for i := range dest {
		if i >= len(row.Values) {
			dest[i] = nil
			continue
		}
		v, err := catalog.Unmarshal(row.Values[i])
		if err != nil {
			return err
		}
		dest[i] = v
	}
	return nil
}
