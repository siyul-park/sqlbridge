package eval

import (
	"errors"
	"strings"
)

type Dispatcher struct {
	fns map[string]Function
}

type Option func(*Dispatcher)

type Function func(args []Value) (Value, error)

func WithFunction(name string, f Function) Option {
	return func(d *Dispatcher) { d.fns[strings.ToLower(name)] = f }
}

func NewDispatcher(opts ...Option) *Dispatcher {
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
