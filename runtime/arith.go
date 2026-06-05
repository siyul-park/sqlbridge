package runtime

import (
	"github.com/pkg/errors"
	"github.com/siyul-park/minivm/interp"
	"github.com/xwb1989/sqlparser"

	"github.com/siyul-park/sqlbridge/value"
)

// ArithmeticFunc builds a binary arithmetic or bitwise operator over two SQL
// values. NULL operands yield NULL. Arity 2.
func ArithmeticFunc(op string) *interp.HostFunction {
	return HostFunc(2, func(args []value.Value) (value.Value, error) {
		if args[0] == nil || args[1] == nil {
			return nil, nil
		}
		return arithmetic(op, args[0], args[1])
	})
}

func arithmetic(op string, lhs, rhs value.Value) (value.Value, error) {
	switch op {
	case sqlparser.BitAndStr, sqlparser.BitOrStr, sqlparser.BitXorStr,
		sqlparser.ShiftLeftStr, sqlparser.ShiftRightStr, sqlparser.ModStr:
		return integerOp(op, lhs, rhs)
	case sqlparser.DivStr:
		return floatOp(op, lhs, rhs)
	case sqlparser.PlusStr, sqlparser.MinusStr, sqlparser.MultStr, sqlparser.IntDivStr:
		return numericOp(op, lhs, rhs)
	default:
		return nil, errors.Errorf("runtime: unsupported operator %q", op)
	}
}

// numericOp keeps integer results when both operands are integers and promotes
// to float otherwise.
func numericOp(op string, lhs, rhs value.Value) (value.Value, error) {
	lp, rp, err := value.Promote(lhs, rhs)
	if err != nil {
		return nil, err
	}
	if _, ok := lp.(*value.Float64); ok {
		return floatOp(op, lp, rp)
	}
	return integerOp(op, lp, rp)
}

func integerOp(op string, lhs, rhs value.Value) (value.Value, error) {
	l, err := value.ToInt(lhs)
	if err != nil {
		return nil, err
	}
	r, err := value.ToInt(rhs)
	if err != nil {
		return nil, err
	}
	switch op {
	case sqlparser.PlusStr:
		return value.NewInt64(l + r), nil
	case sqlparser.MinusStr:
		return value.NewInt64(l - r), nil
	case sqlparser.MultStr:
		return value.NewInt64(l * r), nil
	case sqlparser.IntDivStr, sqlparser.ModStr:
		if r == 0 {
			return nil, nil
		}
		if op == sqlparser.ModStr {
			return value.NewInt64(l % r), nil
		}
		return value.NewInt64(l / r), nil
	case sqlparser.BitAndStr:
		return value.NewInt64(l & r), nil
	case sqlparser.BitOrStr:
		return value.NewInt64(l | r), nil
	case sqlparser.BitXorStr:
		return value.NewInt64(l ^ r), nil
	case sqlparser.ShiftLeftStr:
		return value.NewInt64(l << uint64(r)), nil
	case sqlparser.ShiftRightStr:
		return value.NewInt64(l >> uint64(r)), nil
	default:
		return nil, errors.Errorf("runtime: unsupported integer operator %q", op)
	}
}

func floatOp(op string, lhs, rhs value.Value) (value.Value, error) {
	l, err := value.ToFloat(lhs)
	if err != nil {
		return nil, err
	}
	r, err := value.ToFloat(rhs)
	if err != nil {
		return nil, err
	}
	switch op {
	case sqlparser.PlusStr:
		return value.NewFloat64(l + r), nil
	case sqlparser.MinusStr:
		return value.NewFloat64(l - r), nil
	case sqlparser.MultStr:
		return value.NewFloat64(l * r), nil
	case sqlparser.DivStr:
		if r == 0 {
			return nil, nil
		}
		return value.NewFloat64(l / r), nil
	default:
		return nil, errors.Errorf("runtime: unsupported float operator %q", op)
	}
}
