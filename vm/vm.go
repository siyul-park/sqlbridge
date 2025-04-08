package vm

import (
	"bytes"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/tidwall/gjson"
	"github.com/xwb1989/sqlparser"
)

type VM struct {
}

func New() *VM {
	return &VM{}
}

func (vm *VM) Eval(expr sqlparser.Expr, record schema.Record, args ...driver.NamedValue) (driver.Value, error) {
	switch expr := expr.(type) {
	case *sqlparser.AndExpr:
		return vm.evalAndExpr(expr, record, args...)
	case *sqlparser.OrExpr:
		return vm.evalOrExpr(expr, record, args...)
	case *sqlparser.NotExpr:
		return vm.evalNotExpr(expr, record, args...)
	case *sqlparser.ParenExpr:
		return vm.evalParenExpr(expr, record, args...)
	case *sqlparser.ComparisonExpr:
		return vm.evalComparisonExpr(expr, record, args...)
	case *sqlparser.RangeCond:
		return vm.evalRangeCond(expr, record, args...)
	case *sqlparser.IsExpr:
	case *sqlparser.ExistsExpr:
	case *sqlparser.SQLVal:
		return vm.evalSQLVal(expr, args...)
	case *sqlparser.NullVal:
		return vm.evalNullVal(expr)
	case sqlparser.BoolVal:
		return vm.evalBoolVal(expr)
	case *sqlparser.ColName:
		return vm.evalColName(expr, record)
	case sqlparser.ValTuple:
		return vm.evalValTuple(expr, record, args...)
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

func (vm *VM) evalAndExpr(expr *sqlparser.AndExpr, record schema.Record, args ...driver.NamedValue) (driver.Value, error) {
	left, err := vm.Eval(expr.Left, record, args...)
	if err != nil {
		return nil, err
	}
	if !vm.Bool(left) {
		return false, nil
	}

	right, err := vm.Eval(expr.Right, record, args...)
	if err != nil {
		return nil, err
	}
	if !vm.Bool(right) {
		return false, nil
	}
	return true, nil
}

func (vm *VM) evalOrExpr(expr *sqlparser.OrExpr, record schema.Record, args ...driver.NamedValue) (driver.Value, error) {
	left, err := vm.Eval(expr.Left, record, args...)
	if err != nil {
		return nil, err
	}
	if vm.Bool(left) {
		return true, nil
	}

	right, err := vm.Eval(expr.Right, record, args...)
	if err != nil {
		return nil, err
	}
	if vm.Bool(right) {
		return true, nil
	}
	return false, nil
}

func (vm *VM) evalNotExpr(expr *sqlparser.NotExpr, record schema.Record, args ...driver.NamedValue) (driver.Value, error) {
	val, err := vm.Eval(expr.Expr, record, args...)
	if err != nil {
		return nil, err
	}
	return !vm.Bool(val), nil
}

func (vm *VM) evalParenExpr(expr *sqlparser.ParenExpr, record schema.Record, args ...driver.NamedValue) (driver.Value, error) {
	return vm.Eval(expr.Expr, record, args...)
}

func (vm *VM) evalComparisonExpr(expr *sqlparser.ComparisonExpr, record schema.Record, args ...driver.NamedValue) (driver.Value, error) {
	left, err := vm.Eval(expr.Left, record, args...)
	if err != nil {
		return nil, err
	}

	right, err := vm.Eval(expr.Right, record, args...)
	if err != nil {
		return nil, err
	}

	switch expr.Operator {
	case sqlparser.EqualStr, sqlparser.NullSafeEqualStr:
		return vm.Equal(left, right), nil
	case sqlparser.NotEqualStr:
		return !vm.Equal(left, right), nil
	case sqlparser.LessThanStr:
		return vm.Compare(left, right) < 0, nil
	case sqlparser.LessEqualStr:
		return vm.Compare(left, right) <= 0, nil
	case sqlparser.GreaterThanStr:
		return vm.Compare(left, right) > 0, nil
	case sqlparser.GreaterEqualStr:
		return vm.Compare(left, right) >= 0, nil
	case sqlparser.InStr:
		return vm.In(left, right), nil
	case sqlparser.NotInStr:
		return !vm.In(left, right), nil
	case sqlparser.LikeStr:
		return vm.Like(left, right), nil
	case sqlparser.NotLikeStr:
		return !vm.Like(left, right), nil
	case sqlparser.RegexpStr:
		return vm.Regexp(left, right), nil
	case sqlparser.NotRegexpStr:
		return !vm.Regexp(left, right), nil
	case sqlparser.JSONExtractOp:
		return vm.Extract(left, right), nil
	case sqlparser.JSONUnquoteExtractOp:
		val, err := json.Marshal(vm.Extract(left, right))
		if err != nil {
			return nil, err
		}
		return string(val), nil
	}

	return nil, driver.ErrSkip
}

func (vm *VM) evalRangeCond(expr *sqlparser.RangeCond, record schema.Record, args ...driver.NamedValue) (driver.Value, error) {
	left, err := vm.Eval(expr.Left, record, args...)
	if err != nil {
		return nil, err
	}
	from, err := vm.Eval(expr.From, record, args...)
	if err != nil {
		return nil, err
	}
	to, err := vm.Eval(expr.To, record, args...)
	if err != nil {
		return nil, err
	}

	cmp1 := vm.Compare(left, from)
	cmp2 := vm.Compare(left, to)

	if expr.Operator == sqlparser.BetweenStr {
		return cmp1 >= 0 && cmp2 <= 0, nil
	}
	return cmp1 < 0 || cmp2 > 0, nil
}

func (vm *VM) evalIsExpr(record schema.Record, expr *sqlparser.IsExpr, args ...driver.NamedValue) (driver.Value, error) {
	val, err := vm.Eval(expr.Expr, record, args...)
	if err != nil {
		return nil, err
	}

	switch expr.Operator {
	case sqlparser.IsNullStr:
		return val == nil, nil
	case sqlparser.IsNotNullStr:
		return val != nil, nil
	case sqlparser.IsTrueStr, sqlparser.IsNotFalseStr:
		return vm.Bool(val), nil
	case sqlparser.IsNotTrueStr, sqlparser.IsFalseStr:
		return !vm.Bool(val), nil
	}
	return nil, driver.ErrSkip
}

func (vm *VM) evalSQLVal(expr *sqlparser.SQLVal, args ...driver.NamedValue) (driver.Value, error) {
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
		for _, arg := range args {
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

func (vm *VM) evalNullVal(_ *sqlparser.NullVal) (driver.Value, error) {
	return nil, nil
}

func (vm *VM) evalBoolVal(expr sqlparser.BoolVal) (driver.Value, error) {
	return bool(expr), nil
}

func (vm *VM) evalColName(expr *sqlparser.ColName, record schema.Record) (driver.Value, error) {
	val, _ := record.Get(expr)
	return val, nil
}

func (vm *VM) evalValTuple(expr sqlparser.ValTuple, record schema.Record, args ...driver.NamedValue) ([]driver.Value, error) {
	values := make([]driver.Value, 0, len(expr))
	for _, e := range expr {
		value, err := vm.Eval(e, record, args...)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func (vm *VM) In(lhs, rhs driver.Value) bool {
	rv := reflect.ValueOf(rhs)
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		for i := 0; i < rv.Len(); i++ {
			if vm.Equal(lhs, rv.Index(i).Interface()) {
				return true
			}
		}
		return false
	default:
		return vm.Equal(lhs, rhs)
	}
}

func (vm *VM) Like(lhs, rhs driver.Value) bool {
	value := vm.String(lhs)
	pattern := vm.String(rhs)

	var re strings.Builder
	re.WriteString(`(?i)^`)

	escaped := false
	for _, r := range pattern {
		switch {
		case escaped:
			re.WriteString(regexp.QuoteMeta(string(r)))
			escaped = false
		case r == '\\':
			escaped = true
		case r == '%':
			re.WriteString(".*")
		case r == '_':
			re.WriteString(".")
		default:
			re.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	re.WriteString(`$`)

	match, _ := regexp.MatchString(re.String(), value)
	return match
}

func (vm *VM) Regexp(lhs, rhs driver.Value) bool {
	value := vm.String(lhs)
	pattern := vm.String(rhs)

	matched, _ := regexp.MatchString(pattern, value)
	return matched
}

func (vm *VM) Extract(lhs, rhs driver.Value) driver.Value {
	value := vm.String(lhs)
	path := vm.String(rhs)

	result := gjson.Get(value, path)
	if !result.Exists() {
		return nil
	}
	return result.Value()
}

func (vm *VM) Compare(lhs, rhs driver.Value) int {
	if vm.Equal(lhs, rhs) {
		return 0
	}

	v1 := reflect.ValueOf(lhs)
	v2 := reflect.ValueOf(rhs)

	if v1.Kind() == reflect.Slice && v1.Type().Elem().Kind() == reflect.Uint8 &&
		v2.Kind() == reflect.Slice && v2.Type().Elem().Kind() == reflect.Uint8 {
		return bytes.Compare(v1.Bytes(), v2.Bytes())
	}

	if v1.Kind() == reflect.String || v2.Kind() == reflect.String {
		return strings.Compare(vm.String(lhs), vm.String(rhs))
	}

	f1 := vm.Float64(lhs)
	f2 := vm.Float64(rhs)
	switch {
	case f1 < f2:
		return -1
	case f1 > f2:
		return 1
	default:
		return 0
	}
}

func (vm *VM) Equal(lhs, rhs driver.Value) bool {
	return reflect.DeepEqual(lhs, rhs)
}

func (vm *VM) String(val driver.Value) string {
	if v := reflect.ValueOf(val); v.Kind() == reflect.String {
		return v.String()
	}
	v, err := json.Marshal(val)
	if err != nil {
		return fmt.Sprint(val)
	}
	return string(v)
}

func (vm *VM) Int(val driver.Value) int {
	return int(vm.Float64(val))
}

func (vm *VM) Float64(val driver.Value) float64 {
	if v := reflect.ValueOf(val); v.CanInt() {
		return float64(v.Int())
	} else if v.CanUint() {
		return float64(v.Uint())
	} else if v.CanFloat() {
		return v.Float()
	} else if vm.Bool(val) {
		return 1
	} else {
		return 0
	}
}

func (vm *VM) Bool(val driver.Value) bool {
	v := reflect.ValueOf(val)
	return v.IsValid() && !v.IsZero()
}
