package runtime

import (
	"context"
	"testing"

	"github.com/siyul-park/minivm/instr"
	"github.com/siyul-park/minivm/interp"
	"github.com/siyul-park/minivm/program"
	"github.com/siyul-park/minivm/types"
	"github.com/stretchr/testify/require"
)

// TestMinivmIntegration is the milestone-1 spike: it proves the minivm
// runtime API the rebuild relies on (program construction, execution,
// result extraction, and host-function dispatch) works against the
// pinned module version, independently of any SQL concern.
func TestMinivmIntegration(t *testing.T) {
	t.Run("arithmetic program returns computed value", func(t *testing.T) {
		// 6 * 7
		prog := program.New([]instr.Instruction{
			instr.New(instr.I32_CONST, 6),
			instr.New(instr.I32_CONST, 7),
			instr.New(instr.I32_MUL),
		})

		vm := interp.New(prog)
		defer vm.Close()

		require.NoError(t, vm.Run(context.Background()))

		got, err := vm.Pop()
		require.NoError(t, err)
		require.Equal(t, types.I32(42), got)
	})

	t.Run("host function round-trips a Go callback", func(t *testing.T) {
		typ := &types.FunctionType{
			Params:  []types.Type{types.TypeI32},
			Returns: []types.Type{types.TypeI32},
		}
		double := interp.NewHostFunction(typ, func(_ *interp.Interpreter, params []types.Boxed) ([]types.Boxed, error) {
			return []types.Boxed{types.BoxI32(params[0].I32() * 2)}, nil
		})

		prog := program.New(
			[]instr.Instruction{
				instr.New(instr.I32_CONST, 21),
				instr.New(instr.CONST_GET, 0),
				instr.New(instr.CALL),
			},
			program.WithConstants(double),
		)

		vm := interp.New(prog)
		defer vm.Close()

		require.NoError(t, vm.Run(context.Background()))

		got, err := vm.Pop()
		require.NoError(t, err)
		require.Equal(t, types.I32(42), got)
	})
}
