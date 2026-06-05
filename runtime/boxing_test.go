package runtime

import (
	"context"
	"testing"

	"github.com/siyul-park/minivm/instr"
	"github.com/siyul-park/minivm/interp"
	"github.com/siyul-park/minivm/program"
	"github.com/siyul-park/minivm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/siyul-park/sqlbridge/value"
)

func TestBox(t *testing.T) {
	vm := interp.New(program.New(nil))
	defer vm.Close()

	tests := []struct {
		name string
		in   value.Value
		kind types.Kind
	}{
		{"int64 native", value.NewInt64(7), types.KindI64},
		{"float64 native", value.NewFloat64(2.5), types.KindF64},
		{"bool native", value.True, types.KindI64},
		{"varchar string ref", value.NewVarChar("hello"), types.KindRef},
		{"varbinary typed array ref", value.NewVarBinary([]byte("bin")), types.KindRef},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := Box(vm, tt.in)
			require.NoError(t, err)
			assert.Equal(t, tt.kind, b.Kind())

			got, err := Unbox(vm, b)
			require.NoError(t, err)
			assert.Equal(t, tt.in.Interface(), got.Interface())
		})
	}

	t.Run("tuple round-trips through a native array", func(t *testing.T) {
		in := value.NewTuple([]value.Value{value.NewInt64(1), value.NewVarChar("x")})
		b, err := Box(vm, in)
		require.NoError(t, err)
		assert.Equal(t, types.KindRef, b.Kind())

		got, err := Unbox(vm, b)
		require.NoError(t, err)
		tup, ok := got.(*value.Tuple)
		require.True(t, ok)
		require.Len(t, tup.Values(), 2)
		assert.Equal(t, int64(1), tup.Values()[0].Interface())
		assert.Equal(t, "x", tup.Values()[1].Interface())
	})

	t.Run("nil maps to canonical null", func(t *testing.T) {
		b, err := Box(vm, nil)
		require.NoError(t, err)
		assert.Equal(t, types.BoxedNull, b)

		got, err := Unbox(vm, b)
		require.NoError(t, err)
		assert.Nil(t, got)
	})
}

func TestConstant(t *testing.T) {
	assert.Equal(t, types.I64(5), Constant(value.NewInt64(5)))
	assert.Equal(t, types.F64(1.5), Constant(value.NewFloat64(1.5)))
	assert.Equal(t, types.String("x"), Constant(value.NewVarChar("x")))
}

func TestHostFunc(t *testing.T) {
	t.Run("dispatched from bytecode over native SQL values", func(t *testing.T) {
		double := HostFunc(1, func(args []value.Value) (value.Value, error) {
			n, err := value.ToInt(args[0])
			if err != nil {
				return nil, err
			}
			return value.NewInt64(n * 2), nil
		})

		prog := program.New(
			[]instr.Instruction{
				instr.New(instr.CONST_GET, 0), // push the native i64 argument
				instr.New(instr.CONST_GET, 1), // push the host function
				instr.New(instr.CALL),
			},
			program.WithConstants(Constant(value.NewInt64(21)), double),
		)

		vm := interp.New(prog)
		defer vm.Close()

		require.NoError(t, vm.Run(context.Background()))

		res, err := vm.Pop()
		require.NoError(t, err)
		assert.Equal(t, types.I64(42), res)
	})
}
