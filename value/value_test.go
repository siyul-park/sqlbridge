package value

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

func TestNewValue(t *testing.T) {
	tests := []struct {
		name string
		in   any
		want Value
	}{
		{"int", 42, NewInt64(42)},
		{"int64", int64(42), NewInt64(42)},
		{"uint64", uint64(7), NewUint64(7)},
		{"float64", 3.5, NewFloat64(3.5)},
		{"string", "hello", NewVarChar("hello")},
		{"bytes", []byte("bin"), NewVarBinary([]byte("bin"))},
		{"bool true", true, True},
		{"bool false", false, False},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewValue(tt.in)
			assert.Equal(t, tt.want.Type(), got.Type())
			assert.Equal(t, tt.want.Interface(), got.Interface())
		})
	}
}

func TestCompare(t *testing.T) {
	tests := []struct {
		name string
		lhs  Value
		rhs  Value
		want int
	}{
		{"int equal", NewInt64(5), NewInt64(5), 0},
		{"int less", NewInt64(1), NewInt64(2), -1},
		{"int greater", NewInt64(9), NewInt64(2), 1},
		{"float less", NewFloat64(1.0), NewFloat64(2.0), -1},
		{"varchar equal", NewVarChar("a"), NewVarChar("a"), 0},
		{"varchar less", NewVarChar("a"), NewVarChar("b"), -1},
		{"int vs float promotes", NewInt64(2), NewFloat64(2.0), 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Compare(tt.lhs, tt.rhs)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}

	t.Run("both nil", func(t *testing.T) {
		got, err := Compare(nil, nil)
		require.NoError(t, err)
		assert.Equal(t, 0, got)
	})
}

func TestPromote(t *testing.T) {
	t.Run("int and float become float", func(t *testing.T) {
		l, r, err := Promote(NewInt64(3), NewFloat64(2.5))
		require.NoError(t, err)
		assert.Equal(t, querypb.Type_FLOAT64, l.Type())
		assert.Equal(t, querypb.Type_FLOAT64, r.Type())
	})
}

func TestCast(t *testing.T) {
	t.Run("int to float", func(t *testing.T) {
		got, err := Cast(NewInt64(3), querypb.Type_FLOAT64)
		require.NoError(t, err)
		assert.Equal(t, querypb.Type_FLOAT64, got.Type())
		assert.Equal(t, 3.0, got.Interface())
	})
}

func TestToSQL(t *testing.T) {
	t.Run("round trip int64", func(t *testing.T) {
		sql, err := ToSQL(NewInt64(42), querypb.Type_INT64)
		require.NoError(t, err)
		back, err := FromSQL(sql)
		require.NoError(t, err)
		assert.Equal(t, int64(42), back.Interface())
	})
}

func TestFromSQL(t *testing.T) {
	t.Run("varchar", func(t *testing.T) {
		sql, err := ToSQL(NewVarChar("hi"), querypb.Type_VARCHAR)
		require.NoError(t, err)
		got, err := FromSQL(sql)
		require.NoError(t, err)
		assert.Equal(t, "hi", got.Interface())
	})
}

func TestToBool(t *testing.T) {
	tests := []struct {
		name string
		in   Value
		want bool
	}{
		{"true", True, true},
		{"false", False, false},
		{"nonzero int", NewInt64(5), true},
		{"zero int", NewInt64(0), false},
		{"nil", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ToBool(tt.in))
		})
	}
}

func TestToInt(t *testing.T) {
	got, err := ToInt(NewInt64(10))
	require.NoError(t, err)
	assert.Equal(t, int64(10), got)
}

func TestToFloat(t *testing.T) {
	got, err := ToFloat(NewFloat64(2.5))
	require.NoError(t, err)
	assert.Equal(t, 2.5, got)
}

func TestToString(t *testing.T) {
	got, err := ToString(NewVarChar("x"))
	require.NoError(t, err)
	assert.Equal(t, "x", got)
}

func TestNewBool(t *testing.T) {
	assert.Equal(t, True, NewBool(true))
	assert.Equal(t, False, NewBool(false))
}

func TestNewDateTime(t *testing.T) {
	now := time.Date(2026, 6, 5, 0, 0, 0, 0, time.UTC)
	v := NewDateTime(now)
	assert.Equal(t, querypb.Type_DATETIME, v.Type())
	assert.Equal(t, now, v.Interface())
}
