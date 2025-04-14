package eval

import (
	"errors"
	"math"
)

func WithBuiltIn() Option {
	return func(d *Dispatcher) {
		d.fns["substr"] = NewSubstr()
		d.fns["group_concat"] = NewGroupConcat()
		d.fns["bit_and"] = NewBitAnd()
		d.fns["bit_or"] = NewBitOr()
		d.fns["bit_xor"] = NewBitXor()
		d.fns["count"] = NewCount()
		d.fns["avg"] = NewAvg()
		d.fns["max"] = NewMax()
		d.fns["min"] = NewMin()
		d.fns["std"] = NewStdDevSamp()
		d.fns["stddev"] = NewStdDevSamp()
		d.fns["stddev_samp"] = NewStdDevSamp()
		d.fns["stddev_pop"] = NewStddevPop()
		d.fns["variance"] = NewVarSamp()
		d.fns["var_samp"] = NewVarSamp()
		d.fns["var_pop"] = NewVarPop()
	}
}

func NewSubstr() Function {
	return func(args []Value) (Value, error) {
		if len(args) == 0 {
			return NewString(""), nil
		}

		str, err := ToString(args[0])
		if err != nil {
			return nil, err
		}

		offset, length := int64(0), int64(len(str))
		if len(args) > 1 {
			if offset, err = ToInt(args[1]); err != nil {
				return nil, err
			}
		}
		if len(args) > 2 {
			if length, err = ToInt(args[2]); err != nil {
				return nil, err
			}
		}

		offset = (offset + int64(len(str))) % int64(len(str))
		if offset < 0 {
			offset += int64(len(str))
		}
		length = max(0, length)
		if offset+length > int64(len(str)) {
			length = int64(len(str)) - offset
		}
		return NewString(str[offset : offset+length]), nil
	}
}

func NewGroupConcat() Function {
	return func(args []Value) (Value, error) {
		var value string
		for _, arg := range args {
			str, err := ToString(arg)
			if err != nil {
				return nil, err
			}
			value += str
		}
		return NewString(value), nil
	}
}

func NewBitAnd() Function {
	return func(args []Value) (Value, error) {
		var value uint64
		for i, arg := range args {
			val, err := ToUint(arg)
			if err != nil {
				return nil, err
			}
			if i == 0 {
				value = val
			} else {
				value &= val
			}
		}
		return NewUint64(value), nil
	}
}

func NewBitOr() Function {
	return func(args []Value) (Value, error) {
		var value uint64
		for i, arg := range args {
			val, err := ToUint(arg)
			if err != nil {
				return nil, err
			}
			if i == 0 {
				value = val
			} else {
				value |= val
			}
		}
		return NewUint64(value), nil
	}
}

func NewBitXor() Function {
	return func(args []Value) (Value, error) {
		var value uint64
		for i, arg := range args {
			val, err := ToUint(arg)
			if err != nil {
				return nil, err
			}
			if i == 0 {
				value = val
			} else {
				value ^= val
			}
		}
		return NewUint64(value), nil
	}
}

func NewCount() Function {
	return func(args []Value) (Value, error) {
		return NewInt64(int64(len(args))), nil
	}
}

func NewAvg() Function {
	return func(args []Value) (Value, error) {
		var sum float64
		var count int64
		for _, arg := range args {
			val, err := ToFloat(arg)
			if err != nil {
				return nil, err
			}
			sum += val
			count++
		}
		if count == 0 {
			return nil, nil
		}
		return NewFloat64(sum / float64(count)), nil
	}
}

func NewMax() Function {
	return func(args []Value) (Value, error) {
		if len(args) == 0 {
			return NewInt64(0), nil
		}
		var value int64
		for i, arg := range args {
			val, err := ToInt(arg)
			if err != nil {
				return nil, err
			}
			if i == 0 || val > value {
				value = val
			}
		}
		return NewInt64(value), nil
	}
}

func NewMin() Function {
	return func(args []Value) (Value, error) {
		if len(args) == 0 {
			return NewInt64(0), nil
		}
		var value int64
		for i, arg := range args {
			val, err := ToInt(arg)
			if err != nil {
				return nil, err
			}
			if i == 0 || val < value {
				value = val
			}
		}
		return NewInt64(value), nil
	}
}

func NewSum() Function {
	return func(args []Value) (Value, error) {
		var value int64
		for _, arg := range args {
			val, err := ToInt(arg)
			if err != nil {
				return nil, err
			}
			value += val
		}
		return NewInt64(value), nil
	}
}

func NewStdDevSamp() Function {
	return func(args []Value) (Value, error) {
		var sum float64
		var count int64
		for _, arg := range args {
			val, err := ToFloat(arg)
			if err != nil {
				return nil, err
			}
			sum += val
			count++
		}

		if count < 2 {
			return nil, errors.New("at least two values are required")
		}

		mean := sum / float64(count)
		var sds float64
		for _, arg := range args {
			val, err := ToFloat(arg)
			if err != nil {
				return nil, err
			}
			sds += (val - mean) * (val - mean)
		}
		return NewFloat64(math.Sqrt(sds / float64(count-1))), nil
	}
}

func NewStddevPop() Function {
	return func(args []Value) (Value, error) {
		var sum float64
		var count int64
		for _, arg := range args {
			val, err := ToFloat(arg)
			if err != nil {
				return nil, err
			}
			sum += val
			count++
		}

		if count == 0 {
			return nil, errors.New("no values provided")
		}

		mean := sum / float64(count)
		var sds float64
		for _, arg := range args {
			val, err := ToFloat(arg)
			if err != nil {
				return nil, err
			}
			sds += (val - mean) * (val - mean)
		}
		return NewFloat64(math.Sqrt(sds / float64(count))), nil
	}
}

func NewVarPop() Function {
	return func(args []Value) (Value, error) {
		var sum float64
		var count int64
		for _, arg := range args {
			val, err := ToFloat(arg)
			if err != nil {
				return nil, err
			}
			sum += val
			count++
		}

		if count == 0 {
			return nil, errors.New("no values provided")
		}

		mean := sum / float64(count)
		var sds float64
		for _, arg := range args {
			val, err := ToFloat(arg)
			if err != nil {
				return nil, err
			}
			sds += (val - mean) * (val - mean)
		}
		return NewFloat64(sds / float64(count)), nil
	}
}

func NewVarSamp() Function {
	return func(args []Value) (Value, error) {
		var sum float64
		var count int64
		for _, arg := range args {
			val, err := ToFloat(arg)
			if err != nil {
				return nil, err
			}
			sum += val
			count++
		}

		if count < 2 {
			return nil, errors.New("at least two values are required")
		}

		mean := sum / float64(count)
		var sds float64
		for _, arg := range args {
			val, err := ToFloat(arg)
			if err != nil {
				return nil, err
			}
			sds += (val - mean) * (val - mean)
		}
		return NewFloat64(sds / float64(count-1)), nil
	}
}
