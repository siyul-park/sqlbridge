package value

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

func TestToInt(t *testing.T) {
	tests := []struct {
		name string
		in   Value
		want int64
	}{
		{"int64", NewInt64(7), 7},
		{"uint64", NewUint64(7), 7},
		{"float64", NewFloat64(7.9), 7},
		{"varchar", NewVarChar("42"), 42},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ToInt(tt.in)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestToUint(t *testing.T) {
	got, err := ToUint(NewInt64(5))
	require.NoError(t, err)
	assert.Equal(t, uint64(5), got)
}

func TestToFloat(t *testing.T) {
	tests := []struct {
		name string
		in   Value
		want float64
	}{
		{"int64", NewInt64(3), 3},
		{"float64", NewFloat64(2.5), 2.5},
		{"varchar", NewVarChar("1.5"), 1.5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ToFloat(tt.in)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestToString(t *testing.T) {
	tests := []struct {
		name string
		in   Value
		want string
	}{
		{"varchar", NewVarChar("hi"), "hi"},
		{"int64", NewInt64(9), "9"},
		{"float64", NewFloat64(1.5), "1.5"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ToString(tt.in)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestToBytes(t *testing.T) {
	got, err := ToBytes(NewVarBinary([]byte("abc")))
	require.NoError(t, err)
	assert.Equal(t, []byte("abc"), got)
}

func TestToDateTime(t *testing.T) {
	ts := time.Date(2026, 6, 5, 1, 2, 3, 0, time.UTC)
	got, err := ToDateTime(NewDateTime(ts))
	require.NoError(t, err)
	assert.Equal(t, ts, got)
}

func TestCast(t *testing.T) {
	tests := []struct {
		name string
		in   Value
		typ  querypb.Type
		want any
	}{
		{"int to float", NewInt64(3), querypb.Type_FLOAT64, 3.0},
		{"float to int", NewFloat64(3.9), querypb.Type_INT64, int64(3)},
		{"int to varchar", NewInt64(7), querypb.Type_VARCHAR, "7"},
		{"varchar to int", NewVarChar("12"), querypb.Type_INT64, int64(12)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Cast(tt.in, tt.typ)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got.Interface())
		})
	}
}

func TestCompare_CrossType(t *testing.T) {
	tests := []struct {
		name string
		lhs  Value
		rhs  Value
		want int
	}{
		{"uint equal", NewUint64(5), NewUint64(5), 0},
		{"float vs int", NewFloat64(2.0), NewInt64(3), -1},
		{"varbinary", NewVarBinary([]byte("a")), NewVarBinary([]byte("b")), -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Compare(tt.lhs, tt.rhs)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestNewJSON(t *testing.T) {
	v := NewJSON(map[string]any{"k": "v"})
	assert.Equal(t, querypb.Type_JSON, v.Type())
}

func TestNewTuple(t *testing.T) {
	v := NewTuple([]Value{NewInt64(1), NewInt64(2)})
	assert.Equal(t, querypb.Type_TUPLE, v.Type())
	assert.Len(t, v.Values(), 2)
}
