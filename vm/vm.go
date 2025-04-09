package vm

import (
	"bytes"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/tidwall/gjson"
	"github.com/xwb1989/sqlparser"
)

type VM struct {
	functions  map[string]Function
	converters map[string]Converter
}

type Option func(*VM)

type Function func(args ...driver.Value) (driver.Value, error)
type Converter func(value driver.Value, typ *sqlparser.ConvertType) (driver.Value, error)

func WithFunction(name string, fn Function) Option {
	return func(vm *VM) {
		vm.functions[strings.ToLower(name)] = fn
	}
}

func WithConverter(name string, conv Converter) Option {
	return func(vm *VM) {
		vm.converters[strings.ToLower(name)] = conv
	}
}

func New(opts ...Option) *VM {
	vm := &VM{
		functions:  make(map[string]Function),
		converters: make(map[string]Converter),
	}

	for _, opt := range opts {
		opt(vm)
	}

	vm.functions["upper"] = func(args ...driver.Value) (driver.Value, error) {
		if len(args) < 1 {
			return nil, driver.ErrSkip
		}
		return vm.Upper(args[0]), nil
	}

	vm.functions["lower"] = func(args ...driver.Value) (driver.Value, error) {
		if len(args) < 1 {
			return nil, driver.ErrSkip
		}
		return vm.Lower(args[0]), nil
	}

	vm.functions["substr"] = func(args ...driver.Value) (driver.Value, error) {
		if len(args) < 1 {
			return nil, driver.ErrSkip
		}
		val := args[0]
		var from, to driver.Value
		if len(args) == 2 {
			to = args[1]
		} else if len(args) == 3 {
			from = args[1]
			to = args[2]
		}
		return vm.Substr(val, from, to), nil
	}

	vm.functions["trim"] = func(args ...driver.Value) (driver.Value, error) {
		if len(args) < 1 {
			return "", nil
		}
		return vm.Trim(args[0]), nil
	}

	vm.functions["ltrim"] = func(args ...driver.Value) (driver.Value, error) {
		if len(args) < 1 {
			return "", nil
		}
		return vm.LTrim(args[0]), nil
	}

	vm.functions["rtrim"] = func(args ...driver.Value) (driver.Value, error) {
		if len(args) < 1 {
			return "", nil
		}
		return vm.RTrim(args[0]), nil
	}

	vm.functions["length"] = func(args ...driver.Value) (driver.Value, error) {
		if len(args) < 1 {
			return nil, driver.ErrSkip
		}
		return vm.Length(args[0]), nil
	}

	vm.functions["concat"] = func(args ...driver.Value) (driver.Value, error) {
		return vm.Concat(args), nil
	}

	vm.functions["length"] = func(args ...driver.Value) (driver.Value, error) {
		if len(args) < 1 {
			return nil, driver.ErrSkip
		}
		return vm.Length(args[0]), nil
	}

	vm.functions["abs"] = func(args ...driver.Value) (driver.Value, error) {
		if len(args) < 1 {
			return nil, driver.ErrSkip
		}
		return vm.Abs(args[0]), nil
	}

	vm.functions["ceil"] = func(args ...driver.Value) (driver.Value, error) {
		if len(args) < 1 {
			return nil, driver.ErrSkip
		}
		return vm.Ceil(args[0]), nil
	}

	vm.functions["floor"] = func(args ...driver.Value) (driver.Value, error) {
		if len(args) < 1 {
			return nil, driver.ErrSkip
		}
		return vm.Floor(args[0]), nil
	}

	vm.functions["round"] = func(args ...driver.Value) (driver.Value, error) {
		if len(args) < 1 {
			return nil, driver.ErrSkip
		}
		return vm.Round(args[0]), nil
	}

	vm.functions["sqrt"] = func(args ...driver.Value) (driver.Value, error) {
		if len(args) < 1 {
			return nil, driver.ErrSkip
		}
		return vm.Sqrt(args[0]), nil
	}

	vm.functions["pow"] = func(args ...driver.Value) (driver.Value, error) {
		if len(args) < 2 {
			return nil, driver.ErrSkip
		}
		return vm.Pow(args[0], args[1]), nil
	}

	vm.functions["mod"] = func(args ...driver.Value) (driver.Value, error) {
		if len(args) < 2 {
			return nil, driver.ErrSkip
		}
		return vm.Mod(args[0], args[1]), nil
	}

	vm.functions["is_null"] = func(args ...driver.Value) (driver.Value, error) {
		if len(args) < 1 {
			return nil, driver.ErrSkip
		}
		return vm.IsNull(args[0]), nil
	}

	vm.functions["ifnull"] = func(args ...driver.Value) (driver.Value, error) {
		if len(args) < 2 {
			return nil, driver.ErrSkip
		}
		if vm.IsNull(args[0]) {
			return args[1], nil
		}
		return args[0], nil
	}

	vm.functions["nvl"] = vm.functions["ifnull"]

	vm.functions["nullif"] = func(args ...driver.Value) (driver.Value, error) {
		if len(args) < 2 {
			return nil, driver.ErrSkip
		}
		if vm.Equal(args[0], args[1]) {
			return nil, nil
		}
		return args[0], nil
	}

	vm.functions["coalesce"] = func(args ...driver.Value) (driver.Value, error) {
		return vm.Coalesce(args...), nil
	}

	vm.converters["bool"] = func(value driver.Value, _ *sqlparser.ConvertType) (driver.Value, error) {
		return vm.Bool(value), nil
	}
	for _, name := range []string{"boolean"} {
		vm.converters[name] = vm.converters["bool"]
	}

	vm.converters["int"] = func(value driver.Value, _ *sqlparser.ConvertType) (driver.Value, error) {
		return vm.Int64(value), nil
	}
	for _, name := range []string{"integer", "tinyint", "smallint", "mediumint", "bigint"} {
		vm.converters[name] = vm.converters["int"]
	}

	vm.converters["float"] = func(value driver.Value, _ *sqlparser.ConvertType) (driver.Value, error) {
		return vm.Float64(value), nil
	}
	for _, name := range []string{"double", "real", "decimal", "numeric"} {
		vm.converters[name] = vm.converters["float"]
	}

	vm.converters["char"] = func(value driver.Value, _ *sqlparser.ConvertType) (driver.Value, error) {
		return vm.String(value), nil
	}
	for _, name := range []string{"varchar", "text", "tinytext", "mediumtext", "longtext"} {
		vm.converters[name] = vm.converters["char"]
	}

	vm.converters["binary"] = func(value driver.Value, _ *sqlparser.ConvertType) (driver.Value, error) {
		return vm.Byte(value), nil
	}
	for _, name := range []string{"varbinary", "blob", "tinyblob", "mediumblob", "longblob"} {
		vm.converters[name] = vm.converters["binary"]
	}

	vm.converters["date"] = func(value driver.Value, _ *sqlparser.ConvertType) (driver.Value, error) {
		return vm.Time(value), nil
	}
	for _, name := range []string{"datetime", "timestamp", "time", "year"} {
		vm.converters[name] = vm.converters["date"]
	}

	vm.converters["using"] = func(value driver.Value, typ *sqlparser.ConvertType) (driver.Value, error) {
		return vm.String(value), nil
	}

	return vm
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
		return vm.evalIsExpr(expr, record, args...)
	case *sqlparser.ExistsExpr:
		return vm.evalExistsExpr(expr, record, args...)
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
		return vm.evalListArgExpr(expr, args...)
	case *sqlparser.BinaryExpr:
		return vm.evalBinaryExpr(expr, record, args...)
	case *sqlparser.UnaryExpr:
		return vm.evalUnaryExpr(expr, record, args...)
	case *sqlparser.IntervalExpr:
		return vm.evalIntervalExpr(expr, record, args...)
	case *sqlparser.CollateExpr:
		return vm.evalCollateExpr(expr, record, args...)
	case *sqlparser.FuncExpr:
		return vm.evalFuncExpr(expr, record, args...)
	case *sqlparser.CaseExpr:
		return vm.evalCaseExpr(expr, record, args...)
	case *sqlparser.ValuesFuncExpr:
		return vm.evalValuesFuncExpr(expr, record)
	case *sqlparser.ConvertExpr:
		return vm.evalConvertExpr(expr, record, args...)
	case *sqlparser.SubstrExpr:
		return vm.evalSubstrExpr(expr, record, args...)
	case *sqlparser.ConvertUsingExpr:
		return vm.evalConvertUsingExpr(expr, record, args...)
	case *sqlparser.MatchExpr:
		return vm.evalMatchExpr(expr, record, args...)
	case *sqlparser.GroupConcatExpr:
		return vm.evalGroupConcatExpr(expr, record, args...)
	case *sqlparser.Default:
		return vm.evalDefault(expr)
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

func (vm *VM) evalIsExpr(expr *sqlparser.IsExpr, record schema.Record, args ...driver.NamedValue) (driver.Value, error) {
	val, err := vm.Eval(expr.Expr, record, args...)
	if err != nil {
		return nil, err
	}

	switch expr.Operator {
	case sqlparser.IsNullStr:
		return vm.IsNull(val), nil
	case sqlparser.IsNotNullStr:
		return !vm.IsNull(val), nil
	case sqlparser.IsTrueStr, sqlparser.IsNotFalseStr:
		return vm.Bool(val), nil
	case sqlparser.IsFalseStr, sqlparser.IsNotTrueStr:
		return !vm.Bool(val), nil
	default:
		return nil, driver.ErrSkip
	}
}

func (vm *VM) evalExistsExpr(expr *sqlparser.ExistsExpr, record schema.Record, args ...driver.NamedValue) (driver.Value, error) {
	val, err := vm.Eval(expr.Subquery, record, args...)
	if err != nil {
		return nil, err
	}
	return vm.Bool(val), nil
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
		for _, arg := range args {
			if arg.Name == strings.TrimPrefix(string(expr.Val), ":") {
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

func (vm *VM) evalListArgExpr(expr sqlparser.ListArg, args ...driver.NamedValue) (driver.Value, error) {
	name := strings.TrimPrefix(string(expr), "::")

	for _, arg := range args {
		if arg.Name == name {
			rv := reflect.ValueOf(arg.Value)

			if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
				return nil, nil
			}

			result := make([]driver.Value, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				result[i] = rv.Index(i).Interface()
			}
			return result, nil
		}
	}
	return nil, nil
}

func (vm *VM) evalBinaryExpr(expr *sqlparser.BinaryExpr, record schema.Record, args ...driver.NamedValue) (driver.Value, error) {
	left, err := vm.Eval(expr.Left, record, args...)
	if err != nil {
		return nil, err
	}
	right, err := vm.Eval(expr.Right, record, args...)
	if err != nil {
		return nil, err
	}

	lv := reflect.ValueOf(left)
	rv := reflect.ValueOf(right)

	if expr.Operator == sqlparser.PlusStr {
		if lv.Kind() == reflect.String || rv.Kind() == reflect.String {
			return vm.String(left) + vm.String(right), nil
		}
	}

	lf64 := vm.Float64(left)
	rf64 := vm.Float64(right)

	li := int64(lf64)
	ri := int64(rf64)

	switch expr.Operator {
	case sqlparser.PlusStr:
		return lf64 + rf64, nil
	case sqlparser.MinusStr:
		return lf64 - rf64, nil
	case sqlparser.MultStr:
		return lf64 * rf64, nil
	case sqlparser.DivStr:
		if rf64 == 0 {
			return math.NaN(), nil
		}
		return lf64 / rf64, nil
	case sqlparser.IntDivStr:
		if ri == 0 {
			return math.NaN(), nil
		}
		return float64(li / ri), nil
	case sqlparser.ModStr:
		if ri == 0 {
			return math.NaN(), nil
		}
		return float64(li % ri), nil
	case sqlparser.BitAndStr:
		return float64(li & ri), nil
	case sqlparser.BitOrStr:
		return float64(li | ri), nil
	case sqlparser.BitXorStr:
		return float64(li ^ ri), nil
	case sqlparser.ShiftLeftStr:
		return float64(li << ri), nil
	case sqlparser.ShiftRightStr:
		return float64(li >> ri), nil
	default:
		return nil, driver.ErrSkip
	}
}

func (vm *VM) evalUnaryExpr(expr *sqlparser.UnaryExpr, record schema.Record, args ...driver.NamedValue) (driver.Value, error) {
	value, err := vm.Eval(expr.Expr, record, args...)
	if err != nil {
		return nil, err
	}

	switch expr.Operator {
	case sqlparser.UMinusStr:
		return -vm.Float64(value), nil
	case sqlparser.UPlusStr:
		return vm.Float64(value), nil
	case sqlparser.TildaStr:
		return ^vm.Int64(value), nil
	case sqlparser.BangStr:
		return !vm.Bool(value), nil
	case sqlparser.BinaryStr, sqlparser.UBinaryStr:
		return []byte(vm.String(value)), nil
	default:
		return nil, driver.ErrSkip
	}
}

func (vm *VM) evalIntervalExpr(expr *sqlparser.IntervalExpr, record schema.Record, args ...driver.NamedValue) (driver.Value, error) {
	value, err := vm.Eval(expr.Expr, record, args...)
	if err != nil {
		return nil, err
	}

	num := vm.Int64(value)
	unit := strings.ToLower(expr.Unit)

	switch unit {
	case "microsecond":
		return time.Duration(num) * time.Microsecond, nil
	case "second":
		return time.Duration(num) * time.Second, nil
	case "minute":
		return time.Duration(num) * time.Minute, nil
	case "hour":
		return time.Duration(num) * time.Hour, nil
	case "day":
		return time.Duration(num) * time.Hour * 24, nil
	case "week":
		return time.Duration(num) * time.Hour * 24 * 7, nil
	case "month":
		return time.Duration(num) * time.Hour * 24 * 7 * 365 / 12, nil
	case "quarter":
		return time.Duration(num) * time.Hour * 24 * 365 / 4, nil
	case "year":
		return time.Duration(num) * time.Hour * 24 * 365, nil
	default:
		return nil, driver.ErrSkip
	}
}

func (vm *VM) evalCollateExpr(expr *sqlparser.CollateExpr, record schema.Record, args ...driver.NamedValue) (driver.Value, error) {
	return vm.Eval(expr.Expr, record, args...)
}

func (vm *VM) evalFuncExpr(expr *sqlparser.FuncExpr, record schema.Record, args ...driver.NamedValue) (driver.Value, error) {
	var values []driver.Value
	evalSelectExprs := func(record schema.Record) error {
		for _, se := range expr.Exprs {
			switch e := se.(type) {
			case *sqlparser.AliasedExpr:
				val, err := vm.Eval(e.Expr, record, args...)
				if err != nil {
					return err
				}
				values = append(values, val)
			case *sqlparser.StarExpr:
				values = append(values, record.Values...)
			default:
				return driver.ErrSkip
			}
		}
		return nil
	}

	if expr.IsAggregate() {
		group, _ := record.Get(schema.GroupColumn)
		if records, ok := group.([]schema.Record); ok {
			for _, record := range records {
				if err := evalSelectExprs(record); err != nil {
					return nil, err
				}
			}
		}
	} else {
		if err := evalSelectExprs(record); err != nil {
			return nil, err
		}
	}

	if expr.Distinct {
		var distinct []driver.Value
		for _, v := range values {
			ok := false
			for _, dv := range distinct {
				if vm.Equal(v, dv) {
					ok = true
					break
				}
			}
			if !ok {
				distinct = append(distinct, v)
			}
		}
		values = distinct
	}

	name := strings.ToLower(expr.Name.String())
	return vm.Call(name, values...)
}

func (vm *VM) evalCaseExpr(expr *sqlparser.CaseExpr, record schema.Record, args ...driver.NamedValue) (driver.Value, error) {
	var value driver.Value
	var err error
	if expr.Expr != nil {
		value, err = vm.Eval(expr.Expr, record, args...)
		if err != nil {
			return nil, err
		}
	}

	for _, when := range expr.Whens {
		cond, err := vm.Eval(when.Cond, record, args...)
		if err != nil {
			return nil, err
		}

		match := false
		if expr.Expr != nil {
			match = vm.Equal(value, cond)
		} else {
			match = vm.Bool(cond)
		}

		if match {
			return vm.Eval(when.Val, record, args...)
		}
	}

	if expr.Else != nil {
		return vm.Eval(expr.Else, record, args...)
	}
	return nil, nil
}

func (vm *VM) evalValuesFuncExpr(expr *sqlparser.ValuesFuncExpr, record schema.Record) (driver.Value, error) {
	val, _ := record.Get(expr.Name)
	return val, nil
}

func (vm *VM) evalConvertExpr(expr *sqlparser.ConvertExpr, record schema.Record, args ...driver.NamedValue) (driver.Value, error) {
	val, err := vm.Eval(expr.Expr, record, args...)
	if err != nil {
		return nil, err
	}
	return vm.Convert(expr.Type, val)
}

func (vm *VM) evalSubstrExpr(expr *sqlparser.SubstrExpr, record schema.Record, args ...driver.NamedValue) (driver.Value, error) {
	name, err := vm.Eval(expr.Name, record, args...)
	if err != nil {
		return nil, err
	}
	var from driver.Value
	if expr.From != nil {
		from, err = vm.Eval(expr.From, record, args...)
		if err != nil {
			return nil, err
		}
	}
	var to driver.Value
	if expr.To != nil {
		to, err = vm.Eval(expr.To, record, args...)
		if err != nil {
			return nil, err
		}
	}
	return vm.Substr(name, from, to), nil
}

func (vm *VM) evalConvertUsingExpr(expr *sqlparser.ConvertUsingExpr, record schema.Record, args ...driver.NamedValue) (driver.Value, error) {
	val, err := vm.Eval(expr.Expr, record, args...)
	if err != nil {
		return nil, err
	}
	typ := &sqlparser.ConvertType{Type: "using", Charset: expr.Type}
	return vm.Convert(typ, val)
}

func (vm *VM) evalMatchExpr(_ *sqlparser.MatchExpr, _ schema.Record, _ ...driver.NamedValue) (driver.Value, error) {
	return nil, driver.ErrSkip
}

func (vm *VM) evalGroupConcatExpr(_ *sqlparser.GroupConcatExpr, _ schema.Record, _ ...driver.NamedValue) (driver.Value, error) {
	return nil, driver.ErrSkip
}

func (vm *VM) evalDefault(_ *sqlparser.Default) (driver.Value, error) {
	return nil, nil
}

func (vm *VM) Call(name string, args ...driver.Value) (driver.Value, error) {
	fn, ok := vm.functions[name]
	if !ok {
		return nil, driver.ErrSkip
	}
	return fn(args...)
}

func (vm *VM) Convert(typ *sqlparser.ConvertType, value driver.Value) (driver.Value, error) {
	conv, ok := vm.converters[typ.Type]
	if !ok {
		return nil, driver.ErrSkip
	}
	return conv(value, typ)
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

func (vm *VM) Upper(val driver.Value) string {
	return strings.ToUpper(vm.String(val))
}

func (vm *VM) Lower(val driver.Value) string {
	return strings.ToLower(vm.String(val))
}

func (vm *VM) Substr(val, from, to driver.Value) string {
	name := vm.String(val)
	offset := int(vm.Int64(from))

	var length int
	if to != nil {
		length = int(vm.Int64(to))
	} else {
		length = len([]rune(name))
	}

	runes := []rune(name)
	start := offset - 1
	if start < 0 {
		start = 0
	}
	end := start + length
	if end > len(runes) {
		end = len(runes)
	}
	if start > len(runes) {
		return ""
	}
	return string(runes[start:end])
}

func (vm *VM) Trim(val driver.Value) string {
	return strings.TrimSpace(vm.String(val))
}

func (vm *VM) LTrim(val driver.Value) string {
	return strings.TrimLeft(vm.String(val), " \t\n\r")
}

func (vm *VM) RTrim(val driver.Value) string {
	return strings.TrimRight(vm.String(val), " \t\n\r")
}

func (vm *VM) Concat(args []driver.Value) string {
	var b strings.Builder
	for _, arg := range args {
		b.WriteString(vm.String(arg))
	}
	return b.String()
}

func (vm *VM) Length(val driver.Value) int {
	if vm.IsNull(val) {
		return 0
	}

	rv := reflect.ValueOf(val)
	if !rv.IsValid() {
		return 0
	}

	for rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return 0
		}
		rv = rv.Elem()
	}

	switch rv.Kind() {
	case reflect.String, reflect.Array, reflect.Slice, reflect.Map:
		if rv.IsValid() {
			return rv.Len()
		}
	default:
		str := vm.String(val)
		if str == "" {
			return 0
		}
		return len(str)
	}
	return 0
}

func (vm *VM) Abs(val driver.Value) float64 {
	f := vm.Float64(val)
	return math.Abs(f)
}

func (vm *VM) Round(val driver.Value) float64 {
	f := vm.Float64(val)
	return math.Round(f)
}

func (vm *VM) Floor(val driver.Value) float64 {
	f := vm.Float64(val)
	return math.Floor(f)
}

func (vm *VM) Ceil(val driver.Value) float64 {
	f := vm.Float64(val)
	return math.Ceil(f)
}

func (vm *VM) Sqrt(val driver.Value) float64 {
	return math.Sqrt(vm.Float64(val))
}

func (vm *VM) Pow(base, exp driver.Value) float64 {
	return math.Pow(vm.Float64(base), vm.Float64(exp))
}

func (vm *VM) Mod(a, b driver.Value) float64 {
	return math.Mod(vm.Float64(a), vm.Float64(b))
}

func (vm *VM) IsNull(val driver.Value) bool {
	if val == nil {
		return true
	}
	rv := reflect.ValueOf(val)
	if !rv.IsValid() {
		return true
	}
	if rv.Kind() == reflect.Ptr && rv.IsNil() {
		return true
	}
	return false
}

func (vm *VM) Coalesce(args ...driver.Value) driver.Value {
	for _, val := range args {
		if !vm.IsNull(val) {
			return val
		}
	}
	return nil
}

func (vm *VM) Compare(lhs, rhs driver.Value) int {
	if vm.Equal(lhs, rhs) {
		return 0
	}

	lv := reflect.ValueOf(lhs)
	rv := reflect.ValueOf(rhs)

	if lv.Kind() == reflect.Slice && lv.Type().Elem().Kind() == reflect.Uint8 &&
		rv.Kind() == reflect.Slice && rv.Type().Elem().Kind() == reflect.Uint8 {
		return bytes.Compare(lv.Bytes(), rv.Bytes())
	}

	if lv.Kind() == reflect.String || rv.Kind() == reflect.String {
		return strings.Compare(vm.String(lhs), vm.String(rhs))
	}

	lf64 := vm.Float64(lhs)
	rf64 := vm.Float64(rhs)
	switch {
	case lf64 < rf64:
		return -1
	case lf64 > rf64:
		return 1
	default:
		return 0
	}
}

func (vm *VM) Equal(lhs, rhs driver.Value) bool {
	return reflect.DeepEqual(lhs, rhs)
}

func (vm *VM) Bool(val driver.Value) bool {
	v := reflect.ValueOf(val)
	return v.IsValid() && !v.IsZero()
}

func (vm *VM) Int64(val driver.Value) int64 {
	return int64(vm.Float64(val))
}

func (vm *VM) Float64(val driver.Value) float64 {
	if v := reflect.ValueOf(val); v.CanInt() {
		return float64(v.Int())
	} else if v.CanUint() {
		return float64(v.Uint())
	} else if v.CanFloat() {
		return v.Float()
	} else if v.Kind() == reflect.String {
		f64, _ := strconv.ParseFloat(vm.String(val), 64)
		return f64
	} else if vm.Bool(val) {
		return 1
	} else {
		return 0
	}
}

func (vm *VM) String(val driver.Value) string {
	if val == nil {
		return ""
	}
	if s, ok := val.(fmt.Stringer); ok {
		return s.String()
	}

	v := reflect.ValueOf(val)
	switch v.Kind() {
	case reflect.String:
		return v.String()
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return string(v.Bytes())
		}
	case reflect.Array:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			b := make([]byte, v.Len())
			for i := 0; i < v.Len(); i++ {
				b[i] = byte(v.Index(i).Uint())
			}
			return string(b)
		}
	default:
	}

	b, err := json.Marshal(val)
	if err != nil {
		return fmt.Sprint(val)
	}
	return string(b)
}

func (vm *VM) Byte(val driver.Value) []byte {
	return []byte(vm.String(val))
}

func (vm *VM) Time(val driver.Value) time.Time {
	if vm.IsNull(val) {
		return time.Time{}
	}

	rv := reflect.ValueOf(val)
	switch {
	case rv.Kind() >= reflect.Int && rv.Kind() <= reflect.Int64:
		return time.Unix(rv.Int(), 0)
	case rv.Kind() >= reflect.Uint && rv.Kind() <= reflect.Uint64:
		return time.Unix(int64(rv.Uint()), 0)
	case rv.Kind() == reflect.Float32 || rv.Kind() == reflect.Float64:
		return time.Unix(int64(rv.Float()), 0)
	}

	layouts := []string{
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02",
		"02 Jan 2006 15:04",
	}
	str := vm.String(val)
	for _, layout := range layouts {
		if t, err := time.Parse(layout, str); err == nil {
			return t
		}
	}

	return time.Unix(int64(vm.Int64(val)), 0)
}
