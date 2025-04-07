package vm

import (
	"database/sql/driver"
	"encoding/hex"
	"reflect"
	"strconv"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser"
)

type VM struct {
	args []driver.NamedValue
}

func New(args ...driver.NamedValue) *VM {
	return &VM{args: args}
}

func (vm *VM) Eval(record schema.Record, expr sqlparser.Expr) (driver.Value, error) {
	switch expr := expr.(type) {
	case *sqlparser.AndExpr:
	case *sqlparser.OrExpr:
	case *sqlparser.NotExpr:
	case *sqlparser.ParenExpr:
	case *sqlparser.ComparisonExpr:
		return vm.evalComparisonExpr(record, expr)
	case *sqlparser.RangeCond:
	case *sqlparser.IsExpr:
	case *sqlparser.ExistsExpr:
	case *sqlparser.SQLVal:
		return vm.evalSQLVal(record, expr)
	case *sqlparser.NullVal:
		return vm.evalNullVal(record, expr)
	case sqlparser.BoolVal:
		return vm.evalBoolVal(record, expr)
	case *sqlparser.ColName:
		return vm.evalColName(record, expr)
	case sqlparser.ValTuple:
	case sqlparser.ListArg:
	case *sqlparser.BinaryExpr:
	case *sqlparser.UnaryExpr:
	case *sqlparser.IntervalExpr:
	case *sqlparser.CollateExpr:
	case *sqlparser.FuncExpr:
	case *sqlparser.CaseExpr:
	case *sqlparser.ValuesFuncExpr:
	case *sqlparser.ConvertExpr:
	case *sqlparser.SubstrExpr:
	case *sqlparser.ConvertUsingExpr:
	case *sqlparser.MatchExpr:
	case *sqlparser.GroupConcatExpr:
	case *sqlparser.Default:
	}
	return nil, driver.ErrSkip
}

func (vm *VM) evalComparisonExpr(record schema.Record, expr *sqlparser.ComparisonExpr) (driver.Value, error) {
	left, err := vm.Eval(record, expr.Left)
	if err != nil {
		return nil, err
	}

	right, err := vm.Eval(record, expr.Right)
	if err != nil {
		return nil, err
	}

	switch expr.Operator {
	case sqlparser.EqualStr, sqlparser.NullSafeEqualStr:
		return reflect.DeepEqual(left, right), nil
	case sqlparser.LessEqualStr, sqlparser.GreaterEqualStr:
		if !reflect.DeepEqual(left, right) {
			return false, nil
		}
	case sqlparser.NotEqualStr:
		return !reflect.DeepEqual(left, right), nil
	}

	switch expr.Operator {
	case sqlparser.LessThanStr, sqlparser.LessEqualStr:
	case sqlparser.GreaterThanStr, sqlparser.GreaterEqualStr:
	case sqlparser.InStr:
	case sqlparser.NotInStr:
	case sqlparser.LikeStr:
	case sqlparser.NotLikeStr:
	case sqlparser.RegexpStr:
	case sqlparser.NotRegexpStr:
	case sqlparser.JSONExtractOp:
	case sqlparser.JSONUnquoteExtractOp:
	}
	return nil, driver.ErrSkip
}

func (vm *VM) evalSQLVal(_ schema.Record, expr *sqlparser.SQLVal) (driver.Value, error) {
	switch expr.Type {
	case sqlparser.StrVal:
		return string(expr.Val), nil
	case sqlparser.IntVal:
		v, err := strconv.ParseInt(string(expr.Val), 10, 64)
		if err != nil {
			return nil, err
		}
		return v, nil
	case sqlparser.FloatVal:
		v, err := strconv.ParseFloat(string(expr.Val), 64)
		if err != nil {
			return nil, err
		}
		return v, nil
	case sqlparser.HexNum:
		v, err := strconv.ParseInt(string(expr.Val), 16, 64)
		if err != nil {
			return nil, err
		}
		return v, nil
	case sqlparser.HexVal:
		v, err := hex.DecodeString(string(expr.Val))
		if err != nil {
			return nil, err
		}
		return v, nil
	case sqlparser.ValArg:
		if len(expr.Val) == 1 {
			return nil, driver.ErrSkip
		}
		for _, arg := range vm.args {
			if arg.Name == string(expr.Val[1:]) {
				return arg.Value, nil
			}
		}
		return nil, nil
	case sqlparser.BitVal:
		var buf []byte
		for i := len(string(expr.Val)); i > 0; i -= 8 {
			var chunk string
			if i-8 < 0 {
				chunk = string(expr.Val)[0:i]
			} else {
				chunk = string(expr.Val)[i-8 : i]
			}
			v, err := strconv.ParseUint(chunk, 2, 8)
			if err != nil {
				return nil, err
			}
			buf = append([]byte{byte(v)}, buf...)
		}
		return buf, nil
	}
	return nil, driver.ErrSkip
}

func (vm *VM) evalNullVal(_ schema.Record, _ *sqlparser.NullVal) (driver.Value, error) {
	return nil, nil
}

func (vm *VM) evalBoolVal(_ schema.Record, expr sqlparser.BoolVal) (driver.Value, error) {
	return bool(expr), nil
}

func (vm *VM) evalColName(record schema.Record, expr *sqlparser.ColName) (driver.Value, error) {
	val, _ := record.Get(expr)
	return val, nil
}
