package compile

import (
	"context"
	"testing"

	"github.com/siyul-park/minivm/instr"
	"github.com/siyul-park/minivm/interp"
	"github.com/siyul-park/minivm/program"
	"github.com/siyul-park/minivm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuilder_Build(t *testing.T) {
	t.Run("forward and backward branches drive a counted loop", func(t *testing.T) {
		const n = 5 // sum of 0..n-1 == 10

		b := NewBuilder()
		// acc = 0; i = 0
		b.Emit(instr.I32_CONST, 0).Emit(instr.GLOBAL_SET, 0)
		b.Emit(instr.I32_CONST, 0).Emit(instr.GLOBAL_SET, 1)

		cond := b.Label()
		done := b.Label()

		b.Bind(cond)
		// if i >= n goto done
		b.Emit(instr.GLOBAL_GET, 1).Emit(instr.I32_CONST, n).Emit(instr.I32_GE_S)
		b.Branch(instr.BR_IF, done)
		// acc += i
		b.Emit(instr.GLOBAL_GET, 0).Emit(instr.GLOBAL_GET, 1).Emit(instr.I32_ADD).Emit(instr.GLOBAL_SET, 0)
		// i += 1
		b.Emit(instr.GLOBAL_GET, 1).Emit(instr.I32_CONST, 1).Emit(instr.I32_ADD).Emit(instr.GLOBAL_SET, 1)
		b.Branch(instr.BR, cond)

		b.Bind(done)
		b.Emit(instr.GLOBAL_GET, 0)

		insts, err := b.Build()
		require.NoError(t, err)

		vm := interp.New(program.New(insts), interp.WithGlobals(2))
		defer vm.Close()
		require.NoError(t, vm.Run(context.Background()))

		res, err := vm.Pop()
		require.NoError(t, err)
		assert.Equal(t, types.I32(10), res)
	})

	t.Run("unbound label is rejected", func(t *testing.T) {
		b := NewBuilder()
		dangling := b.Label()
		b.Branch(instr.BR, dangling)
		_, err := b.Build()
		assert.Error(t, err)
	})
}
