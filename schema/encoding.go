package schema

import (
	"encoding/json"
	"reflect"
	"strconv"

	"github.com/xwb1989/sqlparser/dependency/sqltypes"
)

// Marshal safely converts a JSON-encoded interface into a sqltypes.Value.
func Marshal(value any) (sqltypes.Value, error) {
	switch v := reflect.ValueOf(value); v.Kind() {
	case reflect.Ptr:
		if v.IsNil() {
			return sqltypes.NULL, nil
		}
		return Marshal(v.Elem().Interface())
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return sqltypes.MakeTrusted(sqltypes.VarBinary, v.Bytes()), nil
		}
		d, err := json.Marshal(value)
		if err != nil {
			return sqltypes.NULL, err
		}
		return sqltypes.MakeTrusted(sqltypes.TypeJSON, d), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return sqltypes.NewInt64(v.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return sqltypes.NewUint64(v.Uint()), nil
	case reflect.Float32, reflect.Float64:
		return sqltypes.NewFloat64(v.Float()), nil
	case reflect.String:
		return sqltypes.NewVarChar(v.String()), nil
	case reflect.Invalid:
		return sqltypes.NULL, nil
	default:
		d, err := json.Marshal(value)
		if err != nil {
			return sqltypes.NULL, err
		}
		return sqltypes.MakeTrusted(sqltypes.TypeJSON, d), nil
	}
}

// Unmarshal converts sqltypes.Value into a Go value of type any.
func Unmarshal(value sqltypes.Value) (any, error) {
	if value.IsNull() {
		return nil, nil
	}

	switch value.Type() {
	case sqltypes.Int8, sqltypes.Int16, sqltypes.Int24, sqltypes.Int32, sqltypes.Int64:
		return strconv.ParseInt(string(value.Raw()), 10, 64)
	case sqltypes.Uint8, sqltypes.Uint16, sqltypes.Uint24, sqltypes.Uint32, sqltypes.Uint64:
		return strconv.ParseUint(string(value.Raw()), 10, 64)
	case sqltypes.Float32, sqltypes.Float64, sqltypes.Decimal:
		return strconv.ParseFloat(string(value.Raw()), 64)
	case sqltypes.VarChar, sqltypes.Text, sqltypes.Char:
		return value.ToString(), nil
	case sqltypes.TypeJSON:
		var v any
		if err := json.Unmarshal(value.Raw(), &v); err != nil {
			return nil, err
		}
		return v, nil
	case sqltypes.Blob, sqltypes.VarBinary:
		return value.Raw(), nil
	default:
		return value.ToString(), nil
	}
}
