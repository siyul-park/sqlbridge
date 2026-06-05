package runtime

import (
	"github.com/siyul-park/minivm/interp"
	"github.com/siyul-park/minivm/types"

	"github.com/siyul-park/sqlbridge/value"
)

// SetBinds attaches bind-variable values to the session for one execution.
func (s *Session) SetBinds(binds map[string]value.Value) {
	s.binds = binds
}

// BindFunc resolves a bind variable (e.g. ":v1") to its value. A missing bind
// yields NULL. Arity 0.
func BindFunc(name string) *interp.HostFunction {
	return interp.NewHostFunction(refToRef, func(vm *interp.Interpreter, _ []types.Boxed) ([]types.Boxed, error) {
		sess, err := session(vm)
		if err != nil {
			return nil, err
		}
		ref, err := Box(vm, sess.binds[name])
		if err != nil {
			return nil, err
		}
		return []types.Boxed{ref}, nil
	})
}
