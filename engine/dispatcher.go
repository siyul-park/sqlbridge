package engine

import (
	"errors"
	"fmt"
	"strings"
)

type Dispatcher struct {
	fns map[string]Function
}

type DispatchOption func(*Dispatcher)

type Function func(args []Value) (Value, error)

func WithFunction(name string, f Function) DispatchOption {
	return func(d *Dispatcher) { d.fns[strings.ToLower(name)] = f }
}

func NewTernaryFunction(fn func(a, b, c Value) (Value, error)) Function {
	return func(args []Value) (Value, error) {
		if len(args) != 3 {
			return nil, fmt.Errorf("operator requires exactly 3 arguments")
		}
		return fn(args[0], args[1], args[2])
	}
}

func NewBinaryFunction(fn func(lhs, rhs Value) (Value, error)) Function {
	return func(args []Value) (Value, error) {
		if len(args) != 2 {
			return nil, fmt.Errorf("operator requires exactly 2 arguments")
		}
		return fn(args[0], args[1])
	}
}

func NewDispatcher(opts ...DispatchOption) *Dispatcher {
	d := &Dispatcher{fns: make(map[string]Function)}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

func (d *Dispatcher) Dispatch(name string, args []Value) (Value, error) {
	fn, ok := d.fns[strings.ToLower(name)]
	if !ok {
		return nil, errors.New("function not found: " + name)
	}
	return fn(args)
}
