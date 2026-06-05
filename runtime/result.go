package runtime

import (
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"

	"github.com/siyul-park/sqlbridge/catalog"
	"github.com/siyul-park/sqlbridge/value"
)

// Result accumulates the rows produced by a compiled query program. The program
// pushes one value per output column and commits a row at a time; the driver
// reads the finished rows after execution.
type Result struct {
	columns []*sqlparser.ColName
	current []sqltypes.Value
	rows    []catalog.Row
}

// NewResult creates an accumulator for the given output columns.
func NewResult(columns []*sqlparser.ColName) *Result {
	return &Result{columns: columns}
}

// Push appends a single value to the row currently being built.
func (r *Result) Push(v value.Value) error {
	sv, err := toSQL(v)
	if err != nil {
		return err
	}
	r.current = append(r.current, sv)
	return nil
}

// Commit finalizes the current row and starts a new one.
func (r *Result) Commit() {
	r.rows = append(r.rows, catalog.Row{
		Columns: append([]*sqlparser.ColName(nil), r.columns...),
		Values:  r.current,
	})
	r.current = nil
}

// EmitRow copies an entire input row into the result, used for SELECT *.
func (r *Result) EmitRow(row catalog.Row) {
	r.rows = append(r.rows, catalog.Row{
		Columns: append([]*sqlparser.ColName(nil), row.Columns...),
		Values:  append([]sqltypes.Value(nil), row.Values...),
	})
}

// Limit applies an OFFSET/row-count window to the accumulated rows. A negative
// count means no row-count limit.
func (r *Result) Limit(offset, count int64) {
	if offset < 0 {
		offset = 0
	}
	if offset >= int64(len(r.rows)) {
		r.rows = nil
		return
	}
	r.rows = r.rows[offset:]
	if count >= 0 && count < int64(len(r.rows)) {
		r.rows = r.rows[:count]
	}
}

// Rows returns the accumulated rows.
func (r *Result) Rows() []catalog.Row {
	return r.rows
}

// toSQL converts a SQL value to its sqltypes form, mapping nil to NULL.
func toSQL(v value.Value) (sqltypes.Value, error) {
	if v == nil {
		return sqltypes.NULL, nil
	}
	return value.ToSQL(v, v.Type())
}
