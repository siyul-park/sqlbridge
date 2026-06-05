package runtime

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/siyul-park/minivm/interp"
	"github.com/siyul-park/minivm/types"

	"github.com/siyul-park/sqlbridge/value"
)

// cell wraps objects that have no native minivm representation: SQL values
// outside the common int/float/string/null set (Uint64, VarBinary, DateTime,
// Interval, JSON, Tuple) and host execution state (cursors, rows, result
// accumulators). Common SQL values are carried as native types.Value instead,
// so the VM and its JIT see real i64/f64/string values rather than opaque refs.
type cell struct{ obj any }

var _ types.Value = (*cell)(nil)

func (c *cell) Kind() types.Kind { return types.KindRef }
func (c *cell) Type() types.Type { return types.TypeRef }

func (c *cell) String() string {
	if c.obj == nil {
		return "sql<NULL>"
	}
	return fmt.Sprintf("sql<%v>", c.obj)
}

// Wrap stores an arbitrary host object on the VM heap and returns a reference.
// It is used for execution state and for SQL values without a native carrier.
func Wrap(vm *interp.Interpreter, obj any) (types.Boxed, error) {
	addr, err := vm.Alloc(&cell{obj: obj})
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return types.BoxRef(addr), nil
}

// Unwrap returns the host object behind a reference produced by Wrap.
func Unwrap(vm *interp.Interpreter, b types.Boxed) (any, error) {
	if b.Kind() != types.KindRef {
		return nil, errors.Errorf("runtime: expected reference, got %s", b.Kind())
	}
	if b.Ref() == int(types.Null) {
		return nil, nil
	}
	raw, err := vm.Load(b.Ref())
	if err != nil {
		return nil, errors.WithStack(err)
	}
	c, ok := raw.(*cell)
	if !ok {
		return nil, errors.Errorf("runtime: heap ref %d is not a wrapped object (%T)", b.Ref(), raw)
	}
	return c.obj, nil
}

// Constant maps a SQL value to a program constant. Common values become native
// minivm constants (i64/f64/string); others fall back to a heap cell. A nil
// value (SQL NULL) becomes a null cell.
func Constant(v value.Value) types.Value {
	switch t := v.(type) {
	case nil:
		return &cell{obj: nil}
	case *value.Int64:
		return types.I64(t.Int())
	case *value.Float64:
		return types.F64(t.Float())
	case *value.VarChar:
		return types.String(t.String())
	case *value.VarBinary:
		return bytesToArray(t.Bytes())
	default:
		return &cell{obj: v}
	}
}

// Box stores a SQL value for the VM stack, using a native representation where
// possible and a heap cell otherwise. A nil value maps to the canonical null.
func Box(vm *interp.Interpreter, v value.Value) (types.Boxed, error) {
	switch t := v.(type) {
	case nil:
		return types.BoxedNull, nil
	case *value.Int64:
		return types.BoxI64(t.Int()), nil
	case *value.Float64:
		return types.BoxF64(t.Float()), nil
	case *value.VarChar:
		addr, err := vm.Alloc(types.String(t.String()))
		if err != nil {
			return 0, errors.WithStack(err)
		}
		return types.BoxRef(addr), nil
	case *value.VarBinary:
		addr, err := vm.Alloc(bytesToArray(t.Bytes()))
		if err != nil {
			return 0, errors.WithStack(err)
		}
		return types.BoxRef(addr), nil
	case *value.Tuple:
		arr, err := tupleToArray(vm, t)
		if err != nil {
			return 0, err
		}
		addr, err := vm.Alloc(arr)
		if err != nil {
			return 0, errors.WithStack(err)
		}
		return types.BoxRef(addr), nil
	default:
		return Wrap(vm, v)
	}
}

func bytesToArray(b []byte) types.TypedArray[int8] {
	out := make(types.TypedArray[int8], len(b))
	for i, c := range b {
		out[i] = int8(c)
	}
	return out
}

func tupleToArray(vm *interp.Interpreter, t *value.Tuple) (*types.Array, error) {
	vals := t.Values()
	elems := make([]types.Boxed, len(vals))
	for i, e := range vals {
		b, err := Box(vm, e)
		if err != nil {
			return nil, err
		}
		elems[i] = b
	}
	return types.NewArray(types.NewArrayType(types.TypeRef), elems...), nil
}

// Unbox reconstructs a SQL value from a boxed stack value produced by Box, a
// native constant, or a host-function result. The canonical null unboxes to nil.
func Unbox(vm *interp.Interpreter, b types.Boxed) (value.Value, error) {
	switch b.Kind() {
	case types.KindI64:
		return value.NewInt64(b.I64()), nil
	case types.KindI32:
		return value.NewInt64(int64(b.I32())), nil
	case types.KindF64:
		return value.NewFloat64(b.F64()), nil
	case types.KindF32:
		return value.NewFloat64(float64(b.F32())), nil
	case types.KindRef:
		if b.Ref() == int(types.Null) {
			return nil, nil
		}
		raw, err := vm.Load(b.Ref())
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return fromHeap(vm, raw, b.Ref())
	default:
		return nil, errors.Errorf("runtime: unsupported boxed kind %s", b.Kind())
	}
}

// fromHeap reconstructs a SQL value from a heap object behind a reference.
func fromHeap(vm *interp.Interpreter, raw types.Value, ref int) (value.Value, error) {
	switch t := raw.(type) {
	case types.String:
		return value.NewVarChar(string(t)), nil
	case types.I64:
		return value.NewInt64(int64(t)), nil
	case types.F64:
		return value.NewFloat64(float64(t)), nil
	case types.TypedArray[int8]:
		return value.NewVarBinary(arrayToBytes(t)), nil
	case *types.Array:
		return arrayToTuple(vm, t)
	case *cell:
		if t.obj == nil {
			return nil, nil
		}
		sv, ok := t.obj.(value.Value)
		if !ok {
			return nil, errors.Errorf("runtime: reference %d holds %T, not a SQL value", ref, t.obj)
		}
		return sv, nil
	default:
		return nil, errors.Errorf("runtime: reference %d holds unsupported %T", ref, raw)
	}
}

func arrayToBytes(a types.TypedArray[int8]) []byte {
	out := make([]byte, len(a))
	for i, c := range a {
		out[i] = byte(c)
	}
	return out
}

func arrayToTuple(vm *interp.Interpreter, a *types.Array) (value.Value, error) {
	vals := make([]value.Value, len(a.Elems))
	for i, e := range a.Elems {
		v, err := Unbox(vm, e)
		if err != nil {
			return nil, err
		}
		vals[i] = v
	}
	return value.NewTuple(vals), nil
}
