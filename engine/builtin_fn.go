package engine

import (
	"errors"
	"math"
	"strings"

	"github.com/xwb1989/sqlparser"
)

var (
	BitAnd     = sqlparser.NewColIdent("bit_and")
	BitOr      = sqlparser.NewColIdent("bit_or")
	BitXor     = sqlparser.NewColIdent("bit_xor")
	Substr     = sqlparser.NewColIdent("substr")
	ConcatWs   = sqlparser.NewColIdent("concat_ws")
	NVL        = sqlparser.NewColIdent("nvl")
	NVL2       = sqlparser.NewColIdent("nvl2")
	Count      = sqlparser.NewColIdent("count")
	Avg        = sqlparser.NewColIdent("avg")
	Max        = sqlparser.NewColIdent("max")
	Min        = sqlparser.NewColIdent("min")
	Sum        = sqlparser.NewColIdent("sum")
	Std        = sqlparser.NewColIdent("std")
	Stddev     = sqlparser.NewColIdent("stddev")
	StddevSamp = sqlparser.NewColIdent("stddev_samp")
	StddevPop  = sqlparser.NewColIdent("stddev_pop")
	Variance   = sqlparser.NewColIdent("variance")
	VarSamp    = sqlparser.NewColIdent("var_samp")
	VarPop     = sqlparser.NewColIdent("var_pop")
)

func WithBuiltIn() DispatchOption {
	return func(d *Dispatcher) {
		d.fns[BitAnd.String()] = NewBitAnd()
		d.fns[BitOr.String()] = NewBitOr()
		d.fns[BitXor.String()] = NewBitXor()
		d.fns[Substr.String()] = NewSubstr()
		d.fns[ConcatWs.String()] = NewConcatWs()
		d.fns[NVL.String()] = NewNVL()
		d.fns[NVL2.String()] = NewNVL2()
		d.fns[Count.String()] = NewCount()
		d.fns[Avg.String()] = NewAvg()
		d.fns[Max.String()] = NewMax()
		d.fns[Min.String()] = NewMin()
		d.fns[Sum.String()] = NewSum()
		d.fns[Std.String()] = NewStdDevSamp()
		d.fns[Stddev.String()] = NewStdDevSamp()
		d.fns[StddevSamp.String()] = NewStdDevSamp()
		d.fns[StddevPop.String()] = NewStddevPop()
		d.fns[Variance.String()] = NewVarSamp()
		d.fns[VarSamp.String()] = NewVarSamp()
		d.fns[VarPop.String()] = NewVarPop()
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

func NewSubstr() Function {
	return func(args []Value) (Value, error) {
		if len(args) == 0 {
			return NewVarChar(""), nil
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
		return NewVarChar(str[offset : offset+length]), nil
	}
}

func NewConcatWs() Function {
	return func(args []Value) (Value, error) {
		if len(args) == 0 {
			return NewVarChar(""), nil
		}

		sep, err := ToString(args[0])
		if err != nil {
			return nil, err
		}

		var elems []string
		for _, arg := range args[1:] {
			elem, err := ToString(arg)
			if err != nil {
				return nil, err
			}
			elems = append(elems, elem)
		}
		return NewVarChar(strings.Join(elems, sep)), nil
	}
}

func NewNVL() Function {
	return NewBinaryFunction(func(lhs, rhs Value) (Value, error) {
		if lhs == nil {
			return rhs, nil
		}
		return lhs, nil
	})
}

func NewNVL2() Function {
	return NewTernaryFunction(func(x1, x2, x3 Value) (Value, error) {
		if x1 != nil {
			return x2, nil
		}
		return x3, nil
	})
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
