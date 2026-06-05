package runtime

import (
	"github.com/siyul-park/minivm/interp"
	"github.com/siyul-park/minivm/types"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"

	"github.com/siyul-park/sqlbridge/catalog"
)

// Affected returns the number of rows mutated by a write program.
func (s *Session) Affected() int64 { return s.affected }

// InsertFunc builds a row from the pushed column values and inserts it. Its
// arity is the number of columns. Returns NULL.
func InsertFunc(w catalog.Writer, columns []*sqlparser.ColName) *interp.HostFunction {
	arity := len(columns)
	params := make([]types.Type, arity)
	for i := range params {
		params[i] = types.TypeRef
	}
	typ := &types.FunctionType{Params: params, Returns: []types.Type{types.TypeRef}}
	return interp.NewHostFunction(typ, func(vm *interp.Interpreter, boxed []types.Boxed) ([]types.Boxed, error) {
		sess, err := session(vm)
		if err != nil {
			return nil, err
		}
		vals := make([]sqltypes.Value, len(boxed))
		for i, b := range boxed {
			v, err := Unbox(vm, b)
			if err != nil {
				return nil, err
			}
			if vals[i], err = toSQL(v); err != nil {
				return nil, err
			}
		}
		row := catalog.Row{Columns: append([]*sqlparser.ColName(nil), columns...), Values: vals}
		n, err := w.Insert(vm.Context(), row)
		if err != nil {
			return nil, err
		}
		sess.affected += n
		return []types.Boxed{types.BoxedNull}, nil
	})
}

// DeleteCurrentFunc deletes the session's current row. Arity 0. Returns NULL.
func DeleteCurrentFunc(w catalog.Writer) *interp.HostFunction {
	return interp.NewHostFunction(refToRef, func(vm *interp.Interpreter, _ []types.Boxed) ([]types.Boxed, error) {
		sess, err := session(vm)
		if err != nil {
			return nil, err
		}
		n, err := w.Delete(vm.Context(), sess.row)
		if err != nil {
			return nil, err
		}
		sess.affected += n
		return []types.Boxed{types.BoxedNull}, nil
	})
}

// UpdateCurrentFunc replaces the named columns of the session's current row with
// the pushed values, applied as a delete of the old row and an insert of the
// new one. Its arity is the number of assigned columns. Returns NULL.
func UpdateCurrentFunc(w catalog.Writer, columns []*sqlparser.ColName) *interp.HostFunction {
	arity := len(columns)
	params := make([]types.Type, arity)
	for i := range params {
		params[i] = types.TypeRef
	}
	typ := &types.FunctionType{Params: params, Returns: []types.Type{types.TypeRef}}
	return interp.NewHostFunction(typ, func(vm *interp.Interpreter, boxed []types.Boxed) ([]types.Boxed, error) {
		sess, err := session(vm)
		if err != nil {
			return nil, err
		}

		old := sess.row
		next := catalog.Row{
			Columns: append([]*sqlparser.ColName(nil), old.Columns...),
			Values:  append([]sqltypes.Value(nil), old.Values...),
		}
		for i, b := range boxed {
			v, err := Unbox(vm, b)
			if err != nil {
				return nil, err
			}
			sv, err := toSQL(v)
			if err != nil {
				return nil, err
			}
			setColumn(&next, columns[i], sv)
		}

		if _, err := w.Delete(vm.Context(), old); err != nil {
			return nil, err
		}
		n, err := w.Insert(vm.Context(), next)
		if err != nil {
			return nil, err
		}
		sess.affected += n
		return []types.Boxed{types.BoxedNull}, nil
	})
}

// setColumn replaces (or appends) the value for col in row.
func setColumn(row *catalog.Row, col *sqlparser.ColName, v sqltypes.Value) {
	for i, c := range row.Columns {
		if c.Name.Equal(col.Name) {
			row.Values[i] = v
			return
		}
	}
	row.Columns = append(row.Columns, col)
	row.Values = append(row.Values, v)
}
