package schema

import (
	"encoding/json"
	"reflect"

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
	case reflect.Int64:
		return sqltypes.NewInt64(v.Int()), nil
	case reflect.Uint64:
		return sqltypes.NewUint64(v.Uint()), nil
	case reflect.Float64:
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
