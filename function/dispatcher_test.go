package function

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/siyul-park/sqlbridge/value"
)

func TestDispatcher_Dispatch(t *testing.T) {
	d := New(WithFunction("id", func(args []value.Value) (value.Value, error) {
		return args[0], nil
	}))

	t.Run("invokes registered function case-insensitively", func(t *testing.T) {
		got, err := d.Dispatch("ID", []value.Value{value.NewInt64(5)})
		require.NoError(t, err)
		assert.Equal(t, int64(5), got.Interface())
	})

	t.Run("returns ErrNotFound for unknown name", func(t *testing.T) {
		_, err := d.Dispatch("missing", nil)
		assert.ErrorIs(t, err, ErrNotFound)
	})
}

func TestDispatcher_Lookup(t *testing.T) {
	d := New(WithBuiltin())

	t.Run("finds builtin", func(t *testing.T) {
		_, ok := d.Lookup("SUM")
		assert.True(t, ok)
	})

	t.Run("misses unknown", func(t *testing.T) {
		_, ok := d.Lookup("nope")
		assert.False(t, ok)
	})
}

func TestNewBinaryFunction(t *testing.T) {
	fn := NewBinaryFunction(func(lhs, rhs value.Value) (value.Value, error) {
		return lhs, nil
	})
	t.Run("rejects wrong arity", func(t *testing.T) {
		_, err := fn([]value.Value{value.NewInt64(1)})
		assert.Error(t, err)
	})
	t.Run("accepts two args", func(t *testing.T) {
		got, err := fn([]value.Value{value.NewInt64(1), value.NewInt64(2)})
		require.NoError(t, err)
		assert.Equal(t, int64(1), got.Interface())
	})
}

func TestNewTernaryFunction(t *testing.T) {
	fn := NewTernaryFunction(func(a, b, c value.Value) (value.Value, error) {
		return c, nil
	})
	_, err := fn([]value.Value{value.NewInt64(1)})
	assert.Error(t, err)
	got, err := fn([]value.Value{value.NewInt64(1), value.NewInt64(2), value.NewInt64(3)})
	require.NoError(t, err)
	assert.Equal(t, int64(3), got.Interface())
}
