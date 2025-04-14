package eval

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/xwb1989/sqlparser/dependency/querypb"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
)

type Value interface {
	Type() querypb.Type
	Interface() any
}

type Int64 struct{ data int64 }
type Uint64 struct{ data uint64 }
type Float64 struct{ data float64 }
type String struct{ data string }
type Bytes struct{ data []byte }
type DateTime struct{ data time.Time }
type Duration struct {
	amount int64
	unit   string
}
type JSON struct{ data any }
type Tuple struct{ data []Value }

var (
	True  = &Int64{data: 1}
	False = &Int64{data: 0}
)

var (
	_ Value = (*Int64)(nil)
	_ Value = (*Uint64)(nil)
	_ Value = (*Float64)(nil)
	_ Value = (*String)(nil)
	_ Value = (*Bytes)(nil)
	_ Value = (*DateTime)(nil)
	_ Value = (*Duration)(nil)
	_ Value = (*JSON)(nil)
	_ Value = (*Tuple)(nil)
)

func Compare(lhs, rhs Value) (int, error) {
	lhs, rhs, err := Promote(lhs, rhs)
	if err != nil {
		return 0, err
	}

	switch l := lhs.(type) {
	case *Int64:
		r, ok := rhs.(*Int64)
		if !ok {
			return 0, fmt.Errorf("cannot compare Int64 with %T", rhs)
		}
		if l.Int() < r.Int() {
			return -1, nil
		} else if l.Int() > r.Int() {
			return 1, nil
		}
		return 0, nil
	case *Uint64:
		r, ok := rhs.(*Uint64)
		if !ok {
			return 0, fmt.Errorf("cannot compare Uint64 with %T", rhs)
		}
		if l.Uint() < r.Uint() {
			return -1, nil
		} else if l.Uint() > r.Uint() {
			return 1, nil
		}
		return 0, nil
	case *Float64:
		r, ok := rhs.(*Float64)
		if !ok {
			return 0, fmt.Errorf("cannot compare Float64 with %T", rhs)
		}
		if l.Float() < r.Float() {
			return -1, nil
		} else if l.Float() > r.Float() {
			return 1, nil
		}
		return 0, nil
	case *String:
		r, ok := rhs.(*String)
		if !ok {
			return 0, fmt.Errorf("cannot compare String with %T", rhs)
		}
		return strings.Compare(l.String(), r.String()), nil
	case *Bytes:
		r, ok := rhs.(*Bytes)
		if !ok {
			return 0, fmt.Errorf("cannot compare Bytes with %T", rhs)
		}
		return bytes.Compare(l.Bytes(), r.Bytes()), nil
	case *DateTime:
		r, ok := rhs.(*DateTime)
		if !ok {
			return 0, fmt.Errorf("cannot compare DateTime with %T", rhs)
		}
		if l.Time().Before(r.Time()) {
			return -1, nil
		} else if l.Time().After(r.Time()) {
			return 1, nil
		}
		return 0, nil
	case *Duration:
		r, ok := rhs.(*Duration)
		if !ok {
			return 0, fmt.Errorf("cannot compare Duration with %T", rhs)
		}
		if l.Second() < r.Second() {
			return -1, nil
		} else if l.Second() > r.Second() {
			return 1, nil
		}
		return 0, nil
	case *JSON:
		r, ok := rhs.(*JSON)
		if !ok {
			return 0, fmt.Errorf("cannot compare Duration with %T", rhs)
		}
		lb, err := l.Bytes()
		if err != nil {
			return 0, err
		}
		rb, err := r.Bytes()
		if err != nil {
			return 0, err
		}
		return bytes.Compare(lb, rb), nil

	case *Tuple:
		r, ok := rhs.(*Tuple)
		if !ok {
			return 0, fmt.Errorf("cannot compare Tuple with %T", rhs)
		}
		for i := 0; i < len(l.Values()) && i < len(r.Values()); i++ {
			if cmp, err := Compare(l.Values()[i], r.Values()[i]); err != nil {
				return 0, err
			} else if cmp != 0 {
				return cmp, nil
			}
		}
		if len(l.Values()) < len(r.Values()) {
			return -1, nil
		} else if len(l.Values()) > len(r.Values()) {
			return 1, nil
		}
		return 0, nil

	default:
		return 0, fmt.Errorf("unsupported comparison between %T and %T", lhs, rhs)
	}
}

func Promote(lhs, rhs Value) (Value, Value, error) {
	lhsType := lhs.Type()
	rhsType := rhs.Type()

	if lhsType == rhsType {
		return lhs, rhs, nil
	}

	priority := map[querypb.Type]int{
		querypb.Type_INT64:     1,
		querypb.Type_UINT64:    2,
		querypb.Type_FLOAT64:   3,
		querypb.Type_VARCHAR:   4,
		querypb.Type_VARBINARY: 5,
		querypb.Type_DATETIME:  6,
		querypb.Type_JSON:      7,
	}

	lhsPri, ok1 := priority[lhsType]
	rhsPri, ok2 := priority[rhsType]
	if !ok1 || !ok2 {
		return nil, nil, fmt.Errorf("unsupported type promotion: %v, %v", lhsType, rhsType)
	}

	var targetType querypb.Type
	if lhsPri > rhsPri {
		targetType = lhsType
	} else {
		targetType = rhsType
	}

	lv, err := Cast(lhs, targetType)
	if err != nil {
		return nil, nil, err
	}
	rv, err := Cast(rhs, targetType)
	if err != nil {
		return nil, nil, err
	}
	return lv, rv, nil
}

func Cast(val Value, typ querypb.Type) (Value, error) {
	v, err := ToSQL(val, typ)
	if err != nil {
		return nil, err
	}
	return FromSQL(v)
}

func ToSQL(val Value, typ querypb.Type) (sqltypes.Value, error) {
	if val == nil {
		return sqltypes.NULL, nil
	}
	switch {
	case sqltypes.IsSigned(typ):
		v, err := ToInt(val)
		if err != nil {
			return sqltypes.NULL, err
		}
		return sqltypes.MakeTrusted(typ, strconv.AppendInt(nil, v, 10)), nil

	case sqltypes.IsUnsigned(typ):
		v, err := ToUint(val)
		if err != nil {
			return sqltypes.NULL, err
		}
		return sqltypes.MakeTrusted(typ, strconv.AppendUint(nil, v, 10)), nil

	case sqltypes.IsFloat(typ):
		v, err := ToFloat(val)
		if err != nil {
			return sqltypes.NULL, err
		}
		return sqltypes.MakeTrusted(typ, strconv.AppendFloat(nil, v, 'g', -1, 64)), nil

	case typ == querypb.Type_DECIMAL:
		v, err := ToFloat(val)
		if err != nil {
			return sqltypes.NULL, err
		}
		return sqltypes.MakeTrusted(typ, strconv.AppendFloat(nil, v, 'f', -1, 64)), nil

	case typ == querypb.Type_CHAR,
		typ == querypb.Type_VARCHAR,
		typ == querypb.Type_TEXT,
		typ == querypb.Type_ENUM,
		typ == querypb.Type_SET:
		v, err := ToString(val)
		if err != nil {
			return sqltypes.NULL, err
		}
		return sqltypes.MakeTrusted(typ, []byte(v)), nil

	case typ == querypb.Type_BINARY,
		typ == querypb.Type_VARBINARY,
		typ == querypb.Type_BLOB,
		typ == querypb.Type_BIT:
		v, err := ToBytes(val)
		if err != nil {
			return sqltypes.NULL, err
		}
		return sqltypes.MakeTrusted(typ, v), nil

	case typ == querypb.Type_JSON:
		b, err := json.Marshal(val.Interface())
		if err != nil {
			return sqltypes.NULL, err
		}
		return sqltypes.MakeTrusted(typ, b), nil

	case typ == querypb.Type_DATE:
		v, err := ToDate(val)
		if err != nil {
			return sqltypes.NULL, err
		}
		return sqltypes.MakeTrusted(typ, []byte(v.Format(time.DateOnly))), nil

	case typ == querypb.Type_TIME:
		v, err := ToTime(val)
		if err != nil {
			return sqltypes.NULL, err
		}
		return sqltypes.MakeTrusted(typ, []byte(v.Format(time.TimeOnly))), nil

	case typ == querypb.Type_DATETIME, typ == querypb.Type_TIMESTAMP:
		v, err := ToDateTime(val)
		if err != nil {
			return sqltypes.NULL, err
		}
		return sqltypes.MakeTrusted(typ, []byte(v.Format(time.RFC3339))), nil

	case typ == querypb.Type_YEAR:
		v, err := ToYear(val)
		if err != nil {
			return sqltypes.NULL, err
		}
		return sqltypes.MakeTrusted(typ, strconv.AppendInt(nil, v, 10)), nil

	default:
		return sqltypes.NULL, fmt.Errorf("unsupported sql type: %v", val.Type())
	}
}

func FromSQL(val sqltypes.Value) (Value, error) {
	if val.IsNull() {
		return nil, nil
	}

	switch val.Type() {
	case sqltypes.Int8, sqltypes.Int16, sqltypes.Int24, sqltypes.Int32, sqltypes.Int64:
		i, err := strconv.ParseInt(val.String(), 10, 64)
		if err != nil {
			return nil, err
		}
		return &Int64{data: i}, nil

	case sqltypes.Uint8, sqltypes.Uint16, sqltypes.Uint24, sqltypes.Uint32, sqltypes.Uint64:
		u, err := strconv.ParseUint(val.String(), 10, 64)
		if err != nil {
			return nil, err
		}
		return &Uint64{data: u}, nil

	case sqltypes.Float32, sqltypes.Float64:
		f, err := strconv.ParseFloat(val.String(), 64)
		if err != nil {
			return nil, err
		}
		return &Float64{data: f}, nil

	case sqltypes.Decimal:
		f, err := strconv.ParseFloat(val.String(), 64)
		if err != nil {
			return nil, err
		}
		return &Float64{data: f}, nil

	case sqltypes.Char, sqltypes.VarChar, sqltypes.Text,
		sqltypes.Enum, sqltypes.Set:
		return &String{data: val.String()}, nil

	case sqltypes.Binary, sqltypes.VarBinary, sqltypes.Blob,
		sqltypes.Geometry, sqltypes.Bit, sqltypes.Expression:
		return &Bytes{data: val.Raw()}, nil

	case sqltypes.Date:
		t, err := time.Parse(time.DateOnly, val.String())
		if err != nil {
			return nil, err
		}
		return &DateTime{data: t}, nil

	case sqltypes.Timestamp, sqltypes.Datetime:
		layouts := []string{
			"2006-01-02 15:04:05",
			time.RFC3339,
			time.RFC3339Nano,
		}
		var t time.Time
		var err error
		for _, layout := range layouts {
			t, err = time.Parse(layout, val.String())
			if err == nil {
				return &DateTime{data: t}, nil
			}
		}
		return nil, fmt.Errorf("cannot parse datetime: %w", err)

	case sqltypes.Time:
		t, err := time.Parse(time.TimeOnly, val.String())
		if err != nil {
			return nil, err
		}
		return &DateTime{data: t}, nil

	case sqltypes.Year:
		t, err := time.Parse("2006", val.String())
		if err != nil {
			return nil, err
		}
		return &DateTime{data: t}, nil

	case sqltypes.TypeJSON:
		var v any
		if err := json.Unmarshal(val.Raw(), &v); err != nil {
			return nil, err
		}
		return &JSON{data: v}, nil

	default:
		return nil, fmt.Errorf("unsupported sql type: %v", val.Type())
	}
}

func ToBool(val Value) bool {
	if val == nil {
		return false
	}
	switch v := val.(type) {
	case *Int64:
		return v.data != 0
	case *Uint64:
		return v.data != 0
	case *Float64:
		return v.data != 0
	case *String:
		return v.data != ""
	case *Bytes:
		return len(v.data) > 0
	case *DateTime:
		return !v.data.IsZero()
	case *JSON:
		return !reflect.ValueOf(v.data).IsZero()
	case *Tuple:
		return len(v.data) > 0
	default:
		return false
	}
}

func ToInt(val Value) (int64, error) {
	switch v := val.(type) {
	case *Int64:
		return v.Int(), nil
	case *Uint64:
		return int64(v.Uint()), nil
	case *Float64:
		return int64(v.Float()), nil
	case *String:
		return strconv.ParseInt(v.String(), 10, 64)
	case *Bytes:
		return strconv.ParseInt(string(v.Bytes()), 10, 64)
	case *DateTime:
		return v.data.Unix(), nil
	case *JSON:
		b, err := v.Bytes()
		if err != nil {
			return 0, err
		}
		var i int64
		return i, json.Unmarshal(b, &i)
	default:
		return 0, fmt.Errorf("cannot convert %T to signed int", val)
	}
}

func ToUint(val Value) (uint64, error) {
	switch v := val.(type) {
	case *Int64:
		return uint64(v.Int()), nil
	case *Uint64:
		return v.Uint(), nil
	case *Float64:
		return uint64(v.Float()), nil
	case *String:
		return strconv.ParseUint(v.String(), 10, 64)
	case *Bytes:
		return strconv.ParseUint(string(v.Bytes()), 10, 64)
	case *DateTime:
		return uint64(v.data.Unix()), nil
	case *JSON:
		b, err := v.Bytes()
		if err != nil {
			return 0, err
		}
		var u uint64
		return u, json.Unmarshal(b, &u)
	default:
		return 0, fmt.Errorf("cannot convert %T to unsigned int", val)
	}
}

func ToFloat(val Value) (float64, error) {
	switch v := val.(type) {
	case *Int64:
		return float64(v.Int()), nil
	case *Uint64:
		return float64(v.Uint()), nil
	case *Float64:
		return v.Float(), nil
	case *String:
		return strconv.ParseFloat(v.String(), 64)
	case *Bytes:
		return strconv.ParseFloat(string(v.Bytes()), 64)
	case *DateTime:
		return float64(v.data.Unix()), nil
	case *JSON:
		b, err := v.Bytes()
		if err != nil {
			return 0, err
		}
		var f float64
		return f, json.Unmarshal(b, &f)
	default:
		return 0, fmt.Errorf("cannot convert %T to float", val)
	}
}

func ToString(val Value) (string, error) {
	switch v := val.(type) {
	case *Int64:
		return strconv.FormatInt(v.Int(), 10), nil
	case *Uint64:
		return strconv.FormatUint(v.Uint(), 10), nil
	case *Float64:
		return strconv.FormatFloat(v.Float(), 'f', -1, 64), nil
	case *String:
		return v.String(), nil
	case *Bytes:
		return string(v.Bytes()), nil
	case *DateTime:
		return v.Time().Format(time.RFC3339), nil
	case *Duration:
		return v.String(), nil
	case *JSON:
		b, err := v.Bytes()
		if err != nil {
			return "", err
		}
		return string(b), nil
	default:
		return "", fmt.Errorf("cannot convert %T to string", val)
	}
}

func ToBytes(val Value) ([]byte, error) {
	switch v := val.(type) {
	case *Int64:
		return strconv.AppendInt(nil, v.Int(), 10), nil
	case *Uint64:
		return strconv.AppendUint(nil, v.Uint(), 10), nil
	case *Float64:
		return strconv.AppendFloat(nil, v.Float(), 'f', -1, 64), nil
	case *String:
		return []byte(v.String()), nil
	case *Bytes:
		return v.Bytes(), nil
	case *DateTime:
		return []byte(v.Time().Format(time.RFC3339)), nil
	case *Duration:
		return []byte(v.String()), nil
	case *JSON:
		return v.Bytes()
	default:
		return nil, fmt.Errorf("cannot convert %T to bytes", val)
	}
}

func ToDateTime(val Value) (time.Time, error) {
	switch v := val.(type) {
	case *Int64:
		return time.Unix(v.Int(), 0), nil
	case *Uint64:
		return time.Unix(int64(v.Uint()), 0), nil
	case *Float64:
		return time.UnixMilli(int64(v.Float() * 1000)), nil
	case *String:
		return time.Parse(time.RFC3339, v.String())
	case *Bytes:
		return time.Parse(time.RFC3339, string(v.Bytes()))
	case *DateTime:
		return v.Time(), nil
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to TIME", val)
	}
}

func ToDate(val Value) (time.Time, error) {
	switch v := val.(type) {
	case *Int64:
		return time.Unix(v.Int(), 0), nil
	case *Uint64:
		return time.Unix(int64(v.Uint()), 0), nil
	case *Float64:
		return time.UnixMilli(int64(v.Float() * 1000)), nil
	case *String:
		return time.Parse(time.DateOnly, v.String())
	case *Bytes:
		return time.Parse(time.DateOnly, string(v.Bytes()))
	case *DateTime:
		return v.Time(), nil
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to TIME", val)
	}
}

func ToTime(val Value) (time.Time, error) {
	switch v := val.(type) {
	case *Int64:
		return time.Unix(v.Int(), 0), nil
	case *Uint64:
		return time.Unix(int64(v.Uint()), 0), nil
	case *Float64:
		return time.UnixMilli(int64(v.Float() * 1000)), nil
	case *String:
		return time.Parse(time.TimeOnly, v.String())
	case *Bytes:
		return time.Parse(time.TimeOnly, string(v.Bytes()))
	case *DateTime:
		return v.Time(), nil
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to TIME", val)
	}
}

func ToYear(val Value) (int64, error) {
	switch v := val.(type) {
	case *Int64:
		return v.Int(), nil
	case *Uint64:
		return int64(v.Uint()), nil
	case *Float64:
		return int64(v.Float()), nil
	case *String:
		return strconv.ParseInt(v.String(), 10, 64)
	case *Bytes:
		return strconv.ParseInt(string(v.Bytes()), 10, 64)
	case *DateTime:
		return int64(v.Time().Year()), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to YEAR", val)
	}
}

func NewValue(val any) Value {
	v := reflect.ValueOf(val)
	switch {
	case v.Kind() == reflect.Bool:
		return NewBool(v.Bool())
	case v.CanInt():
		return NewInt64(v.Int())
	case v.CanUint():
		return NewUint64(v.Uint())
	case v.CanFloat():
		return NewFloat64(v.Float())
	case v.Kind() == reflect.String:
		return NewString(v.String())
	case v.Kind() == reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return NewBytes(v.Bytes())
		}
	case v.Kind() == reflect.Struct:
		if t, ok := val.(time.Time); ok {
			return NewDateTime(t)
		}
	default:
	}
	return NewJSON(val)
}

func NewBool(b bool) *Int64 {
	if b {
		return True
	}
	return False
}

func NewInt64(i int64) *Int64           { return &Int64{data: i} }
func NewUint64(u uint64) *Uint64        { return &Uint64{data: u} }
func NewFloat64(f float64) *Float64     { return &Float64{data: f} }
func NewString(s string) *String        { return &String{data: s} }
func NewBytes(b []byte) *Bytes          { return &Bytes{data: b} }
func NewDateTime(t time.Time) *DateTime { return &DateTime{data: t} }
func NewDuration(amount int64, unit string) *Duration {
	return &Duration{amount: amount, unit: strings.ToLower(unit)}
}
func NewJSON(j any) *JSON          { return &JSON{data: j} }
func NewTuple(vals []Value) *Tuple { return &Tuple{data: vals} }

func (v *Int64) Type() querypb.Type { return querypb.Type_INT64 }
func (v *Int64) Interface() any     { return v.data }
func (v *Int64) Int() int64         { return v.data }

func (v *Uint64) Type() querypb.Type { return querypb.Type_UINT64 }
func (v *Uint64) Interface() any     { return v.data }
func (v *Uint64) Uint() uint64       { return v.data }

func (v *Float64) Type() querypb.Type { return querypb.Type_FLOAT64 }
func (v *Float64) Interface() any     { return v.data }
func (v *Float64) Float() float64     { return v.data }

func (v *String) Type() querypb.Type { return querypb.Type_VARCHAR }
func (v *String) Interface() any     { return v.data }
func (v *String) String() string     { return v.data }

func (v *Bytes) Type() querypb.Type { return querypb.Type_VARBINARY }
func (v *Bytes) Interface() any     { return v.data }
func (v *Bytes) Bytes() []byte      { return v.data }

func (v *DateTime) Type() querypb.Type { return querypb.Type_DATETIME }
func (v *DateTime) Interface() any     { return v.data }
func (v *DateTime) Time() time.Time    { return v.data }
func (v *DateTime) Add(d *Duration) (*DateTime, error) {
	switch d.Unit() {
	case "years":
		return &DateTime{data: v.data.AddDate(int(d.Amount()), 0, 0)}, nil
	case "months":
		return &DateTime{data: v.data.AddDate(0, int(d.Amount()), 0)}, nil
	case "days":
		return &DateTime{data: v.data.AddDate(0, 0, int(d.Amount()))}, nil
	case "hours":
		return &DateTime{data: v.data.Add(time.Duration(d.Amount()) * time.Hour)}, nil
	case "minutes":
		return &DateTime{data: v.data.Add(time.Duration(d.Amount()) * time.Minute)}, nil
	case "seconds":
		return &DateTime{data: v.data.Add(time.Duration(d.Amount()) * time.Second)}, nil
	default:
		return nil, fmt.Errorf("unsupported duration unit: %s", d.Unit())
	}
}

func (v *Duration) Type() querypb.Type { return querypb.Type_VARCHAR }
func (v *Duration) Interface() any     { return v.String() }
func (v *Duration) String() string     { return fmt.Sprintf("%d %s", v.amount, v.unit) }
func (v *Duration) Amount() int64      { return v.amount }
func (v *Duration) Unit() string       { return v.unit }
func (v *Duration) Scale(factor float64) *Duration {
	return NewDuration(int64(float64(v.amount)*factor), v.unit)
}
func (v *Duration) Second() int64 {
	switch v.unit {
	case "years":
		return v.amount * 365 * 24 * 60 * 60
	case "months":
		return v.amount * 30 * 24 * 60 * 60
	case "days":
		return v.amount * 24 * 60 * 60
	case "hours":
		return v.amount * 60 * 60
	case "minutes":
		return v.amount * 60
	case "seconds":
		return v.amount
	default:
		return 0
	}
}

func (v *JSON) Type() querypb.Type { return querypb.Type_JSON }
func (v *JSON) Interface() any     { return v.data }
func (v *JSON) Bytes() ([]byte, error) {
	return json.Marshal(v.data)
}

func (v *Tuple) Type() querypb.Type { return querypb.Type_TUPLE }
func (v *Tuple) Interface() any {
	out := make([]any, len(v.data))
	for i, e := range v.data {
		if e != nil {
			out[i] = e.Interface()
		}
	}
	return out
}
func (v *Tuple) Values() []Value {
	return v.data
}
