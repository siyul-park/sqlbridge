package runtime

import (
	"github.com/siyul-park/minivm/interp"
	"github.com/siyul-park/minivm/types"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"

	"github.com/siyul-park/sqlbridge/catalog"
)

// OpenJoinFunc materializes the cross product of left and right into the session
// cursor as merged rows (left columns followed by right columns). The join
// predicate is applied afterwards by the compiled WHERE filter. Arity 0.
func OpenJoinFunc(left, right catalog.Table) *interp.HostFunction {
	return interp.NewHostFunction(refToRef, func(vm *interp.Interpreter, _ []types.Boxed) ([]types.Boxed, error) {
		sess, err := session(vm)
		if err != nil {
			return nil, err
		}

		leftCursor, err := left.Scan(vm.Context())
		if err != nil {
			return nil, err
		}
		leftRows, err := catalog.ReadAll(leftCursor)
		if err != nil {
			return nil, err
		}

		rightCursor, err := right.Scan(vm.Context())
		if err != nil {
			return nil, err
		}
		rightRows, err := catalog.ReadAll(rightCursor)
		if err != nil {
			return nil, err
		}

		merged := make([]catalog.Row, 0, len(leftRows)*len(rightRows))
		for _, l := range leftRows {
			for _, r := range rightRows {
				merged = append(merged, mergeRows(l, r))
			}
		}
		sess.cursor = catalog.NewInMemoryCursor(merged)
		return []types.Boxed{types.BoxedNull}, nil
	})
}

// mergeRows concatenates the columns and values of two rows.
func mergeRows(l, r catalog.Row) catalog.Row {
	columns := make([]*sqlparser.ColName, 0, len(l.Columns)+len(r.Columns))
	columns = append(columns, l.Columns...)
	columns = append(columns, r.Columns...)

	values := make([]sqltypes.Value, 0, len(l.Values)+len(r.Values))
	values = append(values, l.Values...)
	values = append(values, r.Values...)

	return catalog.Row{Columns: columns, Values: values}
}
