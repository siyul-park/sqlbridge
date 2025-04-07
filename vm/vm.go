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
	args []driver.NamedValue
}

func New(args ...driver.NamedValue) *VM {
	return &VM{args: args}
}

func (vm *VM) Eval(record schema.Record, expr sqlparser.Expr) (driver.Value, error) {
	switch expr := expr.(type) {
	case *sqlparser.AndExpr:
		return vm.evalAndExpr(record, expr)
	case *sqlparser.OrExpr:
		return vm.evalOrExpr(record, expr)
	case *sqlparser.NotExpr:
		return vm.evalNotExpr(record, expr)
	case *sqlparser.ParenExpr:
		return vm.evalParenExpr(record, expr)
	case *sqlparser.ComparisonExpr:
		return vm.evalComparisonExpr(record, expr)
	case *sqlparser.RangeCond:
		return vm.evalRangeCond(record, expr)
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
		return vm.evalValTuple(record, expr)
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

func (vm *VM) evalAndExpr(record schema.Record, expr *sqlparser.AndExpr) (driver.Value, error) {
	left, err := vm.Eval(record, expr.Left)
	if err != nil {
		return nil, err
	}
	if !reflect.ValueOf(left).IsValid() || reflect.ValueOf(left).IsZero() {
		return false, nil
	}

	right, err := vm.Eval(record, expr.Right)
	if err != nil {
		return nil, err
	}
	if !reflect.ValueOf(right).IsValid() || reflect.ValueOf(right).IsZero() {
		return false, nil
	}
	return true, nil
}

func (vm *VM) evalOrExpr(record schema.Record, expr *sqlparser.OrExpr) (driver.Value, error) {
	left, err := vm.Eval(record, expr.Left)
	if err != nil {
		return nil, err
	}
	if reflect.ValueOf(left).IsValid() && !reflect.ValueOf(left).IsZero() {
		return true, nil
	}

	right, err := vm.Eval(record, expr.Right)
	if err != nil {
		return nil, err
	}
	if reflect.ValueOf(right).IsValid() && !reflect.ValueOf(right).IsZero() {
		return true, nil
	}
	return false, nil
}

func (vm *VM) evalNotExpr(record schema.Record, expr *sqlparser.NotExpr) (driver.Value, error) {
	val, err := vm.Eval(record, expr.Expr)
	if err != nil {
		return nil, err
	}
	return reflect.ValueOf(val).IsZero(), nil
}

func (vm *VM) evalParenExpr(record schema.Record, expr *sqlparser.ParenExpr) (driver.Value, error) {
	return vm.Eval(record, expr.Expr)
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
	case sqlparser.NotEqualStr:
		return !reflect.DeepEqual(left, right), nil
	case sqlparser.LessThanStr:
		return vm.compare(left, right) < 0, nil
	case sqlparser.LessEqualStr:
		return vm.compare(left, right) <= 0, nil
	case sqlparser.GreaterThanStr:
		return vm.compare(left, right) > 0, nil
	case sqlparser.GreaterEqualStr:
		return vm.compare(left, right) >= 0, nil
	case sqlparser.InStr:
		rv := reflect.ValueOf(right)
		switch rv.Kind() {
		case reflect.Slice, reflect.Array:
			for i := 0; i < rv.Len(); i++ {
				if reflect.DeepEqual(left, rv.Index(i).Interface()) {
					return true, nil
				}
			}
			return false, nil
		default:
			return reflect.DeepEqual(left, right), nil
		}
	case sqlparser.NotInStr:
		rv := reflect.ValueOf(right)
		switch rv.Kind() {
		case reflect.Slice, reflect.Array:
			for i := 0; i < rv.Len(); i++ {
				if reflect.DeepEqual(left, rv.Index(i).Interface()) {
					return false, nil
				}
			}
			return true, nil
		default:
			return !reflect.DeepEqual(left, right), nil
		}
	case sqlparser.LikeStr, sqlparser.NotLikeStr:
		pattern := fmt.Sprint(right)
		value := fmt.Sprint(left)

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

		match, err := regexp.MatchString(re.String(), value)
		if err != nil {
			return nil, err
		}
		if expr.Operator == sqlparser.LikeStr {
			return match, nil
		}
		return !match, nil
	case sqlparser.RegexpStr, sqlparser.NotRegexpStr:
		pattern := fmt.Sprint(right)
		value := fmt.Sprint(left)

		matched, err := regexp.MatchString(pattern, value)
		if err != nil {
			return nil, err
		}
		if expr.Operator == sqlparser.RegexpStr {
			return matched, nil
		}
		return !matched, nil
	case sqlparser.JSONExtractOp, sqlparser.JSONUnquoteExtractOp:
		var value string
		if v := reflect.ValueOf(left); v.Kind() == reflect.String {
			value = v.String()
		} else {
			val, err := json.Marshal(left)
			if err != nil {
				return nil, err
			}
			value = string(val)
		}

		path := fmt.Sprint(right)

		result := gjson.Get(value, path)
		if !result.Exists() {
			return nil, nil
		}
		if expr.Operator == sqlparser.JSONUnquoteExtractOp {
			return result.String(), nil
		}
		return result.Value(), nil
	}

	return nil, driver.ErrSkip
}

func (vm *VM) evalRangeCond(record schema.Record, expr *sqlparser.RangeCond) (driver.Value, error) {
	left, err := vm.Eval(record, expr.Left)
	if err != nil {
		return nil, err
	}
	from, err := vm.Eval(record, expr.From)
	if err != nil {
		return nil, err
	}
	to, err := vm.Eval(record, expr.To)
	if err != nil {
		return nil, err
	}

	cmp1 := vm.compare(left, from)
	cmp2 := vm.compare(left, to)

	if expr.Operator == sqlparser.BetweenStr {
		return cmp1 >= 0 && cmp2 <= 0, nil
	}
	return cmp1 < 0 || cmp2 > 0, nil
}

func (vm *VM) evalIsExpr(record schema.Record, expr *sqlparser.IsExpr) (driver.Value, error) {
	val, err := vm.Eval(record, expr.Expr)
	if err != nil {
		return nil, err
	}

	switch expr.Operator {
	case sqlparser.IsNullStr:
		return val == nil, nil
	case sqlparser.IsNotNullStr:
		return val != nil, nil
	case sqlparser.IsTrueStr, sqlparser.IsNotFalseStr:
		if val == nil {
			return false, nil
		}
		return !reflect.ValueOf(val).IsZero(), nil
	case sqlparser.IsNotTrueStr, sqlparser.IsFalseStr:
		if val == nil {
			return true, nil
		}
		return reflect.ValueOf(val).IsZero(), nil
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

func (vm *VM) evalValTuple(record schema.Record, expr sqlparser.ValTuple) ([]driver.Value, error) {
	values := make([]driver.Value, 0, len(expr))
	for _, e := range expr {
		value, err := vm.Eval(record, e)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func (vm *VM) compare(lhs, rhs any) int {
	if reflect.DeepEqual(lhs, rhs) {
		return 0
	}

	v1 := reflect.ValueOf(lhs)
	v2 := reflect.ValueOf(rhs)

	if v1.Kind() == reflect.Slice && v1.Type().Elem().Kind() == reflect.Uint8 &&
		v2.Kind() == reflect.Slice && v2.Type().Elem().Kind() == reflect.Uint8 {
		return bytes.Compare(v1.Bytes(), v2.Bytes())
	}

	if v1.Kind() == reflect.String || v2.Kind() == reflect.String {
		return strings.Compare(fmt.Sprint(lhs), fmt.Sprint(rhs))
	}

	var f1, f2 float64
	if v1.CanInt() {
		f1 = float64(v1.Int())
	} else if v1.CanUint() {
		f1 = float64(v1.Uint())
	} else if v1.CanFloat() {
		f1 = v1.Float()
	}
	if v2.CanInt() {
		f2 = float64(v2.Int())
	} else if v2.CanUint() {
		f2 = float64(v2.Uint())
	} else if v2.CanFloat() {
		f2 = v2.Float()
	}

	switch {
	case f1 < f2:
		return -1
	case f1 > f2:
		return 1
	default:
		return 0
	}
}
