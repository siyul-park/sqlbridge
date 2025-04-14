package eval

import (
	"errors"
	"sync"
)

type Dispatcher struct {
	fns map[string]Function
	mu  sync.RWMutex
}

type Function func(args []Value) (Value, error)

func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		fns: make(map[string]Function),
	}
}

func (d *Dispatcher) Register(name string, fn Function) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.fns[name] = fn
}

func (d *Dispatcher) Unregister(name string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	delete(d.fns, name)
}

func (d *Dispatcher) Dispatch(name string, args []Value) (Value, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	fn, ok := d.fns[name]
	if !ok {
		return nil, errors.New("function not found: " + name)
	}
	return fn(args)
}
