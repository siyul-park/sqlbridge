package schema

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
)

func TestMarshal(t *testing.T) {
	tests := []struct {
		value    any
		expected sqltypes.Value
	}{
		{
			value:    nil,
			expected: sqltypes.NULL,
		},
		{
			value:    int64(123),
			expected: sqltypes.NewInt64(123),
		},
		{
			value:    uint64(456),
			expected: sqltypes.NewUint64(456),
		},
		{
			value:    3.14,
			expected: sqltypes.NewFloat64(3.14),
		},
		{
			value:    "test",
			expected: sqltypes.NewVarChar("test"),
		},
		{
			value:    []byte{1, 2, 3},
			expected: sqltypes.MakeTrusted(sqltypes.VarBinary, []byte{1, 2, 3}),
		},
		{
			value:    map[string]interface{}{"key": "value"},
			expected: sqltypes.MakeTrusted(sqltypes.TypeJSON, []byte(`{"key":"value"}`)),
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprint(tt.value), func(t *testing.T) {
			result, err := Marshal(tt.value)
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestUnmarshal(t *testing.T) {
	tests := []struct {
		value    sqltypes.Value
		expected any
	}{
		{
			value:    sqltypes.NULL,
			expected: nil,
		},
		{
			value:    sqltypes.NewInt64(123),
			expected: int64(123),
		},
		{
			value:    sqltypes.NewUint64(456),
			expected: uint64(456),
		},
		{
			value:    sqltypes.NewFloat64(3.14),
			expected: 3.14,
		},
		{
			value:    sqltypes.NewVarChar("hello"),
			expected: "hello",
		},
		{
			value:    sqltypes.MakeTrusted(sqltypes.VarBinary, []byte{0x01, 0x02}),
			expected: []byte{0x01, 0x02},
		},
		{
			value:    sqltypes.MakeTrusted(sqltypes.TypeJSON, []byte(`{"x":1}`)),
			expected: map[string]any{"x": float64(1)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.value.String(), func(t *testing.T) {
			result, err := Unmarshal(tt.value)
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}
