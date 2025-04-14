package eval

func Builtin() Option {
	return func(d *Dispatcher) {
		d.fns["substr"] = NewSubstr()
	}
}
