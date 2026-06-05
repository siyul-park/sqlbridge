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
	}{
		{"int64", value.NewInt64(7)},
		{"float64", value.NewFloat64(2.5)},
		{"varchar", value.NewVarChar("hello")},
		{"bool", value.True},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := Box(vm, tt.in)
			require.NoError(t, err)
			assert.Equal(t, types.KindRef, b.Kind())

			got, err := Unbox(vm, b)
			require.NoError(t, err)
			assert.Equal(t, tt.in.Interface(), got.Interface())
		})
	}

	t.Run("nil maps to canonical null", func(t *testing.T) {
		b, err := Box(vm, nil)
		require.NoError(t, err)
		assert.Equal(t, types.BoxedNull, b)

		got, err := Unbox(vm, b)
		require.NoError(t, err)
		assert.Nil(t, got)
	})
}

func TestHostFunc(t *testing.T) {
	t.Run("dispatched from bytecode over boxed SQL values", func(t *testing.T) {
		double := HostFunc(1, func(args []value.Value) (value.Value, error) {
			n, err := value.ToInt(args[0])
			if err != nil {
				return nil, err
			}
			return value.NewInt64(n * 2), nil
		})

		arg := &cell{val: value.NewInt64(21)}
		prog := program.New(
			[]instr.Instruction{
				instr.New(instr.CONST_GET, 0), // push the SQL argument reference
				instr.New(instr.CONST_GET, 1), // push the host function
				instr.New(instr.CALL),
			},
			program.WithConstants(arg, double),
		)

		vm := interp.New(prog)
		defer vm.Close()

		require.NoError(t, vm.Run(context.Background()))

		res, err := vm.Pop()
		require.NoError(t, err)

		c, ok := res.(*cell)
		require.True(t, ok, "expected SQL cell, got %T", res)
		assert.Equal(t, int64(42), c.val.Interface())
	})
}
