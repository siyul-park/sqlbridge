package function

import (
	"strings"

	"github.com/pkg/errors"

	"github.com/siyul-park/sqlbridge/value"
)

// ErrNotFound is returned when a function name is not registered.
var ErrNotFound = errors.New("function not found")

// Func is a SQL function over boxed-free SQL values. Aggregates receive the
// full set of grouped argument values; scalar functions receive one row's args.
type Func func(args []value.Value) (value.Value, error)

// Option configures a Dispatcher.
type Option func(*Dispatcher)

// Dispatcher is a case-insensitive registry of SQL functions.
type Dispatcher struct {
	fns map[string]Func
}

// WithFunction registers fn under name (lowercased).
func WithFunction(name string, fn Func) Option {
	return func(d *Dispatcher) { d.fns[strings.ToLower(name)] = fn }
}

// NewBinaryFunction adapts a two-argument function into a Func with arity checking.
func NewBinaryFunction(fn func(lhs, rhs value.Value) (value.Value, error)) Func {
	return func(args []value.Value) (value.Value, error) {
		if len(args) != 2 {
			return nil, errors.New("function requires exactly 2 arguments")
		}
		return fn(args[0], args[1])
	}
}

// NewTernaryFunction adapts a three-argument function into a Func with arity checking.
func NewTernaryFunction(fn func(a, b, c value.Value) (value.Value, error)) Func {
	return func(args []value.Value) (value.Value, error) {
		if len(args) != 3 {
			return nil, errors.New("function requires exactly 3 arguments")
		}
		return fn(args[0], args[1], args[2])
	}
}

// New builds a Dispatcher from the given options.
func New(opts ...Option) *Dispatcher {
	d := &Dispatcher{fns: make(map[string]Func)}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

// Lookup returns the function registered under name, if any.
func (d *Dispatcher) Lookup(name string) (Func, bool) {
	fn, ok := d.fns[strings.ToLower(name)]
	return fn, ok
}

// Dispatch invokes the named function with args.
func (d *Dispatcher) Dispatch(name string, args []value.Value) (value.Value, error) {
	fn, ok := d.Lookup(name)
	if !ok {
		return nil, errors.Wrap(ErrNotFound, name)
	}
	return fn(args)
}
