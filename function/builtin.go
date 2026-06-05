package function

import (
	"math"
	"strings"

	"github.com/pkg/errors"

	"github.com/siyul-park/sqlbridge/value"
)

// Builtin names registered by WithBuiltin.
const (
	NameBitAnd     = "bit_and"
	NameBitOr      = "bit_or"
	NameBitXor     = "bit_xor"
	NameSubstr     = "substr"
	NameConcatWs   = "concat_ws"
	NameNVL        = "nvl"
	NameNVL2       = "nvl2"
	NameCount      = "count"
	NameAvg        = "avg"
	NameMax        = "max"
	NameMin        = "min"
	NameSum        = "sum"
	NameStd        = "std"
	NameStddev     = "stddev"
	NameStddevSamp = "stddev_samp"
	NameStddevPop  = "stddev_pop"
	NameVariance   = "variance"
	NameVarSamp    = "var_samp"
	NameVarPop     = "var_pop"
)

// WithBuiltin registers the default scalar and aggregate SQL functions.
func WithBuiltin() Option {
	return func(d *Dispatcher) {
		reg := map[string]Func{
			NameBitAnd:     BitAnd(),
			NameBitOr:      BitOr(),
			NameBitXor:     BitXor(),
			NameSubstr:     Substr(),
			NameConcatWs:   ConcatWs(),
			NameNVL:        NVL(),
			NameNVL2:       NVL2(),
			NameCount:      Count(),
			NameAvg:        Avg(),
			NameMax:        Max(),
			NameMin:        Min(),
			NameSum:        Sum(),
			NameStd:        StddevSamp(),
			NameStddev:     StddevSamp(),
			NameStddevSamp: StddevSamp(),
			NameStddevPop:  StddevPop(),
			NameVariance:   VarSamp(),
			NameVarSamp:    VarSamp(),
			NameVarPop:     VarPop(),
		}
		for name, fn := range reg {
			d.fns[name] = fn
		}
	}
}

func BitAnd() Func {
	return func(args []value.Value) (value.Value, error) {
		var acc uint64
		for i, arg := range args {
			v, err := value.ToUint(arg)
			if err != nil {
				return nil, err
			}
			if i == 0 {
				acc = v
			} else {
				acc &= v
			}
		}
		return value.NewUint64(acc), nil
	}
}

func BitOr() Func {
	return func(args []value.Value) (value.Value, error) {
		var acc uint64
		for i, arg := range args {
			v, err := value.ToUint(arg)
			if err != nil {
				return nil, err
			}
			if i == 0 {
				acc = v
			} else {
				acc |= v
			}
		}
		return value.NewUint64(acc), nil
	}
}

func BitXor() Func {
	return func(args []value.Value) (value.Value, error) {
		var acc uint64
		for i, arg := range args {
			v, err := value.ToUint(arg)
			if err != nil {
				return nil, err
			}
			if i == 0 {
				acc = v
			} else {
				acc ^= v
			}
		}
		return value.NewUint64(acc), nil
	}
}

func Substr() Func {
	return func(args []value.Value) (value.Value, error) {
		if len(args) == 0 {
			return value.NewVarChar(""), nil
		}

		str, err := value.ToString(args[0])
		if err != nil {
			return nil, err
		}
		if len(str) == 0 {
			return value.NewVarChar(""), nil
		}

		offset, length := int64(0), int64(len(str))
		if len(args) > 1 {
			if offset, err = value.ToInt(args[1]); err != nil {
				return nil, err
			}
		}
		if len(args) > 2 {
			if length, err = value.ToInt(args[2]); err != nil {
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
		return value.NewVarChar(str[offset : offset+length]), nil
	}
}

func ConcatWs() Func {
	return func(args []value.Value) (value.Value, error) {
		if len(args) == 0 {
			return value.NewVarChar(""), nil
		}

		sep, err := value.ToString(args[0])
		if err != nil {
			return nil, err
		}

		var elems []string
		for _, arg := range args[1:] {
			elem, err := value.ToString(arg)
			if err != nil {
				return nil, err
			}
			elems = append(elems, elem)
		}
		return value.NewVarChar(strings.Join(elems, sep)), nil
	}
}

func NVL() Func {
	return NewBinaryFunction(func(lhs, rhs value.Value) (value.Value, error) {
		if lhs == nil {
			return rhs, nil
		}
		return lhs, nil
	})
}

func NVL2() Func {
	return NewTernaryFunction(func(x1, x2, x3 value.Value) (value.Value, error) {
		if x1 != nil {
			return x2, nil
		}
		return x3, nil
	})
}

func Count() Func {
	return func(args []value.Value) (value.Value, error) {
		return value.NewInt64(int64(len(args))), nil
	}
}

func Avg() Func {
	return func(args []value.Value) (value.Value, error) {
		var sum float64
		var count int64
		for _, arg := range args {
			v, err := value.ToFloat(arg)
			if err != nil {
				return nil, err
			}
			sum += v
			count++
		}
		if count == 0 {
			return nil, nil
		}
		return value.NewFloat64(sum / float64(count)), nil
	}
}

func Max() Func {
	return func(args []value.Value) (value.Value, error) {
		if len(args) == 0 {
			return value.NewInt64(0), nil
		}
		var acc int64
		for i, arg := range args {
			v, err := value.ToInt(arg)
			if err != nil {
				return nil, err
			}
			if i == 0 || v > acc {
				acc = v
			}
		}
		return value.NewInt64(acc), nil
	}
}

func Min() Func {
	return func(args []value.Value) (value.Value, error) {
		if len(args) == 0 {
			return value.NewInt64(0), nil
		}
		var acc int64
		for i, arg := range args {
			v, err := value.ToInt(arg)
			if err != nil {
				return nil, err
			}
			if i == 0 || v < acc {
				acc = v
			}
		}
		return value.NewInt64(acc), nil
	}
}

func Sum() Func {
	return func(args []value.Value) (value.Value, error) {
		var acc int64
		for _, arg := range args {
			v, err := value.ToInt(arg)
			if err != nil {
				return nil, err
			}
			acc += v
		}
		return value.NewInt64(acc), nil
	}
}

func StddevSamp() Func {
	return func(args []value.Value) (value.Value, error) {
		_, sds, count, err := dispersion(args)
		if err != nil {
			return nil, err
		}
		if count < 2 {
			return nil, errors.New("at least two values are required")
		}
		return value.NewFloat64(math.Sqrt(sds / float64(count-1))), nil
	}
}

func StddevPop() Func {
	return func(args []value.Value) (value.Value, error) {
		_, sds, count, err := dispersion(args)
		if err != nil {
			return nil, err
		}
		if count == 0 {
			return nil, errors.New("no values provided")
		}
		return value.NewFloat64(math.Sqrt(sds / float64(count))), nil
	}
}

func VarSamp() Func {
	return func(args []value.Value) (value.Value, error) {
		_, sds, count, err := dispersion(args)
		if err != nil {
			return nil, err
		}
		if count < 2 {
			return nil, errors.New("at least two values are required")
		}
		return value.NewFloat64(sds / float64(count-1)), nil
	}
}

func VarPop() Func {
	return func(args []value.Value) (value.Value, error) {
		_, sds, count, err := dispersion(args)
		if err != nil {
			return nil, err
		}
		if count == 0 {
			return nil, errors.New("no values provided")
		}
		return value.NewFloat64(sds / float64(count)), nil
	}
}

// dispersion computes the mean, sum of squared deviations, and count for args.
func dispersion(args []value.Value) (mean, sds float64, count int64, err error) {
	var sum float64
	for _, arg := range args {
		v, err := value.ToFloat(arg)
		if err != nil {
			return 0, 0, 0, err
		}
		sum += v
		count++
	}
	if count == 0 {
		return 0, 0, 0, nil
	}
	mean = sum / float64(count)
	for _, arg := range args {
		v, err := value.ToFloat(arg)
		if err != nil {
			return 0, 0, 0, err
		}
		sds += (v - mean) * (v - mean)
	}
	return mean, sds, count, nil
}
