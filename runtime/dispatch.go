package runtime

import (
	"github.com/siyul-park/minivm/interp"

	"github.com/siyul-park/sqlbridge/function"
	"github.com/siyul-park/sqlbridge/value"
)

// DispatchFunc builds a host function that invokes the named scalar SQL function
// through the dispatcher with arity arguments taken from the stack.
func DispatchFunc(disp *function.Dispatcher, name string, arity int) *interp.HostFunction {
	return HostFunc(arity, func(args []value.Value) (value.Value, error) {
		return disp.Dispatch(name, args)
	})
}
