package plan

import (
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"strconv"
	"time"

	"github.com/xwb1989/sqlparser/dependency/querypb"
)

var NULL = &querypb.BindVariable{Type: querypb.Type_NULL_TYPE}
var TRUE = &querypb.BindVariable{Type: querypb.Type_INT64, Value: []byte("1")}
var FALSE = &querypb.BindVariable{Type: querypb.Type_INT64, Value: []byte("0")}

func ToBool(val any) bool {
	rval := reflect.ValueOf(val)
	switch rval.Kind() {
	case reflect.Bool:
		return rval.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rval.Int() != 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return rval.Uint() != 0
	case reflect.Float32, reflect.Float64:
		return rval.Float() != 0.0
	case reflect.String:
		b, _ := strconv.ParseBool(rval.String())
		return b
	default:
		return false
	}
}

func ToInt(val any) int64 {
	rval := reflect.ValueOf(val)
	switch rval.Kind() {
	case reflect.Bool:
		if rval.Bool() {
			return 1
		}
		return 0
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rval.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(rval.Uint())
	case reflect.Float32, reflect.Float64:
		return int64(rval.Float())
	case reflect.String:
		i, _ := strconv.ParseInt(rval.String(), 10, 64)
		return i
	default:
		return 0
	}
}

func ToFloat(val any) float64 {
	rval := reflect.ValueOf(val)
	switch rval.Kind() {
	case reflect.Bool:
		if rval.Bool() {
			return 1.0
		}
		return 0.0
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(rval.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(rval.Uint())
	case reflect.Float32, reflect.Float64:
		return rval.Float()
	case reflect.String:
		f, _ := strconv.ParseFloat(rval.String(), 64)
		return f
	default:
		return 0.0
	}
}

func ToString(val any) string {
	rval := reflect.ValueOf(val)
	switch rval.Kind() {
	case reflect.Bool:
		return strconv.FormatBool(rval.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(rval.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(rval.Uint(), 10)
	case reflect.Float32, reflect.Float64:
		return strconv.FormatFloat(rval.Float(), 'f', -1, 64)
	case reflect.String:
		return rval.String()
	case reflect.Slice:
		if rval.Type().Elem().Kind() == reflect.Uint8 {
			return string(rval.Bytes())
		}
	default:
	}
	if data, err := json.Marshal(val); err == nil {
		return string(data)
	}
	return fmt.Sprintf("%val", rval.Interface())
}

func Promote(lhs, rhs any) (any, any) {
	lval := reflect.ValueOf(lhs)
	rval := reflect.ValueOf(rhs)

	if lval.Kind() == reflect.String {
		switch rval.Kind() {
		case reflect.Bool:
			return lval.String(), ToString(rval.Interface())
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return lval.String(), ToString(rval.Interface())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return lval.String(), ToString(rval.Interface())
		case reflect.Float32, reflect.Float64:
			return lval.String(), ToString(rval.Interface())
		case reflect.String:
			return lval.String(), rval.String()
		default:
		}
	}

	if rval.Kind() == reflect.String {
		switch lval.Kind() {
		case reflect.Bool:
			return ToString(lval.Interface()), rval.String()
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return ToString(lval.Interface()), rval.String()
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return ToString(lval.Interface()), rval.String()
		case reflect.Float32, reflect.Float64:
			return ToString(lval.Interface()), rval.String()
		case reflect.String:
			return lval.String(), rval.String()
		default:
		}
	}

	if lval.Kind() == reflect.Bool {
		switch rval.Kind() {
		case reflect.String:
			return lval.Bool(), ToBool(rval.Interface())
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return lval.Bool(), ToBool(rval.Interface())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return lval.Bool(), ToBool(rval.Interface())
		case reflect.Float32, reflect.Float64:
			return lval.Bool(), ToBool(rval.Interface())
		default:
		}
	}

	if rval.Kind() == reflect.Bool {
		switch lval.Kind() {
		case reflect.String:
			return ToBool(lval.Interface()), rval.Bool()
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return ToBool(lval.Interface()), rval.Bool()
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return ToBool(lval.Interface()), rval.Bool()
		case reflect.Float32, reflect.Float64:
			return ToBool(lval.Interface()), rval.Bool()
		default:
		}
	}

	if lval.Kind() == reflect.Int || lval.Kind() == reflect.Int8 || lval.Kind() == reflect.Int16 || lval.Kind() == reflect.Int32 || lval.Kind() == reflect.Int64 {
		switch rval.Kind() {
		case reflect.String:
			return lval.Int(), ToInt(rval.Interface())
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return lval.Int(), rval.Int()
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return lval.Int(), int64(rval.Uint())
		case reflect.Float32, reflect.Float64:
			return lval.Int(), int64(rval.Float())
		default:
		}
	}

	if rval.Kind() == reflect.Int || rval.Kind() == reflect.Int8 || rval.Kind() == reflect.Int16 || rval.Kind() == reflect.Int32 || rval.Kind() == reflect.Int64 {
		switch lval.Kind() {
		case reflect.String:
			return ToInt(lval.Interface()), rval.Int()
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return lval.Int(), rval.Int()
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return int64(lval.Uint()), rval.Int()
		case reflect.Float32, reflect.Float64:
			return int64(lval.Float()), rval.Int()
		default:
		}
	}

	if lval.Kind() == reflect.Float32 || lval.Kind() == reflect.Float64 {
		switch rval.Kind() {
		case reflect.String:
			return lval.Float(), ToFloat(rval.Interface())
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return lval.Float(), float64(rval.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return lval.Float(), float64(rval.Uint())
		case reflect.Float32, reflect.Float64:
			return lval.Float(), rval.Float()
		default:
		}
	}

	if rval.Kind() == reflect.Float32 || rval.Kind() == reflect.Float64 {
		switch lval.Kind() {
		case reflect.String:
			return ToFloat(lval.Interface()), rval.Float()
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return float64(lval.Int()), rval.Float()
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return float64(lval.Uint()), rval.Float()
		case reflect.Float32, reflect.Float64:
			return lval.Float(), rval.Float()
		default:
		}
	}

	return lhs, rhs
}

func Marshal(value any) (querypb.Type, []byte, error) {
	v := reflect.ValueOf(value)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if !v.IsValid() {
		return querypb.Type_NULL_TYPE, nil, nil
	}

	switch v.Kind() {
	case reflect.Int8:
		return querypb.Type_INT8, []byte(strconv.FormatInt(v.Int(), 10)), nil
	case reflect.Int16:
		return querypb.Type_INT16, []byte(strconv.FormatInt(v.Int(), 10)), nil
	case reflect.Int32:
		return querypb.Type_INT32, []byte(strconv.FormatInt(v.Int(), 10)), nil
	case reflect.Int64:
		return querypb.Type_INT64, []byte(strconv.FormatInt(v.Int(), 10)), nil
	case reflect.Uint8:
		return querypb.Type_UINT8, []byte(strconv.FormatUint(v.Uint(), 10)), nil
	case reflect.Uint16:
		return querypb.Type_UINT16, []byte(strconv.FormatUint(v.Uint(), 10)), nil
	case reflect.Uint32:
		return querypb.Type_UINT32, []byte(strconv.FormatUint(v.Uint(), 10)), nil
	case reflect.Uint64:
		return querypb.Type_UINT64, []byte(strconv.FormatUint(v.Uint(), 10)), nil
	case reflect.Float32:
		return querypb.Type_FLOAT32, []byte(strconv.FormatFloat(v.Float(), 'f', -1, 32)), nil
	case reflect.Float64:
		return querypb.Type_FLOAT64, []byte(strconv.FormatFloat(v.Float(), 'f', -1, 64)), nil
	case reflect.String:
		return querypb.Type_VARCHAR, []byte(v.String()), nil
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return querypb.Type_BLOB, v.Bytes(), nil
		}
	case reflect.Struct:
		if t, ok := value.(time.Time); ok {
			return querypb.Type_DATETIME, []byte(t.Format(time.DateTime)), nil
		}
		if t, ok := value.(time.Duration); ok {
			return querypb.Type_TIME, []byte(t.String()), nil
		}
		if t, ok := value.(big.Float); ok {
			return querypb.Type_DECIMAL, []byte(t.String()), nil
		}
	default:
	}

	data, err := json.Marshal(value)
	if err != nil {
		return querypb.Type_NULL_TYPE, nil, err
	}
	return querypb.Type_JSON, data, nil
}

func Unmarshal(typ querypb.Type, value []byte) (any, error) {
	if typ == querypb.Type_NULL_TYPE {
		return nil, nil
	}

	switch typ {
	case querypb.Type_INT8:
		i, err := strconv.ParseInt(string(value), 10, 8)
		if err != nil {
			return nil, err
		}
		return int8(i), nil
	case querypb.Type_INT16:
		i, err := strconv.ParseInt(string(value), 10, 16)
		if err != nil {
			return nil, err
		}
		return int16(i), nil
	case querypb.Type_INT24:
		i, err := strconv.ParseInt(string(value), 10, 32)
		if err != nil {
			return nil, err
		}
		return int32(i), nil
	case querypb.Type_INT32:
		i, err := strconv.ParseInt(string(value), 10, 32)
		if err != nil {
			return nil, err
		}
		return int32(i), nil
	case querypb.Type_INT64:
		i, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			return nil, err
		}
		return i, nil
	case querypb.Type_UINT8:
		u, err := strconv.ParseUint(string(value), 10, 8)
		if err != nil {
			return nil, err
		}
		return uint8(u), nil
	case querypb.Type_UINT16:
		u, err := strconv.ParseUint(string(value), 10, 16)
		if err != nil {
			return nil, err
		}
		return uint16(u), nil
	case querypb.Type_UINT24:
		u, err := strconv.ParseUint(string(value), 10, 32)
		if err != nil {
			return nil, err
		}
		return uint32(u), nil
	case querypb.Type_UINT32:
		u, err := strconv.ParseUint(string(value), 10, 32)
		if err != nil {
			return nil, err
		}
		return uint32(u), nil
	case querypb.Type_UINT64:
		u, err := strconv.ParseUint(string(value), 10, 64)
		if err != nil {
			return nil, err
		}
		return u, nil
	case querypb.Type_FLOAT32:
		f, err := strconv.ParseFloat(string(value), 32)
		if err != nil {
			return nil, err
		}
		return float32(f), nil
	case querypb.Type_FLOAT64:
		return strconv.ParseFloat(string(value), 64)
	case querypb.Type_DECIMAL:
		v := string(value)
		f, _, err := big.ParseFloat(v, 10, 256, big.ToNearestEven)
		if err != nil {
			return v, err
		}
		return f, nil
	case querypb.Type_DATE, querypb.Type_DATETIME, querypb.Type_TIMESTAMP, querypb.Type_YEAR:
		v := string(value)
		layouts := []string{
			time.RFC3339,
			time.DateTime,
			time.DateOnly,
			"2006",
		}
		var err error
		for _, layout := range layouts {
			var t time.Time
			t, err = time.ParseInLocation(layout, v, time.Local)
			if err == nil {
				return t, nil
			}
		}
		return nil, err
	case querypb.Type_TIME:
		v := string(value)
		t, err := time.Parse(time.TimeOnly, v)
		if err == nil {
			return t.Sub(time.Time{}), nil
		}
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, err
		}
		return d, nil
	case querypb.Type_TEXT, querypb.Type_VARCHAR, querypb.Type_CHAR, querypb.Type_ENUM, querypb.Type_SET:
		return string(value), nil
	case querypb.Type_BLOB, querypb.Type_VARBINARY, querypb.Type_BINARY, querypb.Type_GEOMETRY, querypb.Type_BIT:
		return value, nil
	case querypb.Type_JSON:
		var result any
		if err := json.Unmarshal(value, &result); err != nil {
			return string(value), nil
		}
		return result, nil
	case querypb.Type_EXPRESSION:
		return string(value), nil
	default:
		return nil, fmt.Errorf("unsupported type: %v", typ)
	}
}
