package runtime

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/siyul-park/minivm/interp"
	"github.com/siyul-park/minivm/types"

	"github.com/siyul-park/sqlbridge/value"
)

// cell wraps a SQL value.Value as a minivm heap object so SQL values can
// flow through the bytecode VM as opaque references. Phase 1 of the rebuild
// uses this uniform reference representation for every SQL value; native
// boxing of numeric columns is a later specialization.
type cell struct{ val value.Value }

var _ types.Value = (*cell)(nil)

func (c *cell) Kind() types.Kind { return types.KindRef }
func (c *cell) Type() types.Type { return types.TypeRef }

func (c *cell) String() string {
	if c.val == nil {
		return "sql<NULL>"
	}
	return fmt.Sprintf("sql<%v>", c.val.Interface())
}

// Box stores a SQL value on the VM heap and returns a boxed reference to it.
// A nil value (SQL NULL) maps to the VM's canonical null reference.
func Box(vm *interp.Interpreter, v value.Value) (types.Boxed, error) {
	if v == nil {
		return types.BoxedNull, nil
	}
	addr, err := vm.Alloc(&cell{val: v})
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return types.BoxRef(addr), nil
}

// Unbox retrieves the SQL value behind a boxed reference produced by Box.
// The canonical null reference unboxes to a nil value (SQL NULL).
func Unbox(vm *interp.Interpreter, b types.Boxed) (value.Value, error) {
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
		return nil, errors.Errorf("runtime: heap ref %d is not a SQL value (%T)", b.Ref(), raw)
	}
	return c.val, nil
}
