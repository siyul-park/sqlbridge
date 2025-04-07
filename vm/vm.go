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
	record schema.Record
}

func Eval(record schema.Record, expr sqlparser.Expr) (driver.Value, error) {
	return New(record).Eval(expr)
}

func New(record schema.Record) *VM {
	return &VM{record: record}
}

func (vm *VM) Eval(expr sqlparser.Expr) (driver.Value, error) {
	switch expr := expr.(type) {
	case *sqlparser.AndExpr:
	case *sqlparser.OrExpr:
	case *sqlparser.NotExpr:
	case *sqlparser.ParenExpr:
	case *sqlparser.ComparisonExpr:
		return vm.evalComparisonExpr(expr)
	case *sqlparser.RangeCond:
	case *sqlparser.IsExpr:
	case *sqlparser.ExistsExpr:
	case *sqlparser.SQLVal:
		return vm.evalSQLVal(expr)
	case *sqlparser.NullVal:
		return vm.evalNullVal(expr)
	case sqlparser.BoolVal:
		return vm.evalBoolVal(expr)
	case *sqlparser.ColName:
		return vm.evalColName(expr)
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

func (vm *VM) evalComparisonExpr(expr *sqlparser.ComparisonExpr) (driver.Value, error) {
	left, err := vm.Eval(expr.Left)
	if err != nil {
		return nil, err
	}

	right, err := vm.Eval(expr.Right)
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

func (vm *VM) evalSQLVal(expr *sqlparser.SQLVal) (driver.Value, error) {
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

func (vm *VM) evalNullVal(_ *sqlparser.NullVal) (driver.Value, error) {
	return nil, nil
}

func (vm *VM) evalBoolVal(expr sqlparser.BoolVal) (driver.Value, error) {
	return bool(expr), nil
}

func (vm *VM) evalColName(expr *sqlparser.ColName) (driver.Value, error) {
	val, _ := vm.record.Get(expr)
	return val, nil
}
