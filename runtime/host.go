package runtime

import (
	"github.com/siyul-park/minivm/interp"
	"github.com/siyul-park/minivm/types"

	"github.com/siyul-park/sqlbridge/value"
)

// HostFunc adapts a SQL-level function (operating on value.Value) into a minivm
// host function using the uniform reference boxing. Parameters arrive boxed,
// are unboxed to SQL values, the function runs in Go, and the single result is
// re-boxed. arity is the number of SQL arguments the function expects.
//
// This is the bridge through which compiled query programs invoke SQL builtins,
// aggregates, and value-semantic operations.
func HostFunc(arity int, fn func([]value.Value) (value.Value, error)) *interp.HostFunction {
	params := make([]types.Type, arity)
	for i := range params {
		params[i] = types.TypeRef
	}
	typ := &types.FunctionType{
		Params:  params,
		Returns: []types.Type{types.TypeRef},
	}

	return interp.NewHostFunction(typ, func(vm *interp.Interpreter, boxed []types.Boxed) ([]types.Boxed, error) {
		args := make([]value.Value, len(boxed))
		for i, b := range boxed {
			v, err := Unbox(vm, b)
			if err != nil {
				return nil, err
			}
			args[i] = v
		}

		res, err := fn(args)
		if err != nil {
			return nil, err
		}

		out, err := Box(vm, res)
		if err != nil {
			return nil, err
		}
		return []types.Boxed{out}, nil
	})
}
