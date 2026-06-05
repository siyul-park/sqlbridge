package runtime

import (
	"context"
	"io"

	"github.com/pkg/errors"
	"github.com/siyul-park/minivm/interp"
	"github.com/siyul-park/minivm/types"
	"github.com/xwb1989/sqlparser"

	"github.com/siyul-park/sqlbridge/catalog"
	"github.com/siyul-park/sqlbridge/value"
)

// GlobalCount is the number of VM global slots a compiled program needs. Per
// execution state travels through the context (see WithSession) rather than
// globals, so none are required.
const GlobalCount = 0

// Session carries the mutable state of one query execution. It lives in Go and
// is reached by host functions through the run context, keeping cursors, the
// current row, and the result accumulator off the VM's reference-counted heap.
type Session struct {
	cursor   catalog.Cursor
	row      catalog.Row
	result   *Result
	grouper  *Grouper
	affected int64
}

// NewSession creates a session writing into result.
func NewSession(result *Result) *Session {
	return &Session{result: result}
}

type sessionKey struct{}

// WithSession attaches a session to a context for a single program execution.
// The driver passes the resulting context to interp.Run; host functions read
// the session back with vm.Context().
func WithSession(ctx context.Context, sess *Session) context.Context {
	return context.WithValue(ctx, sessionKey{}, sess)
}

func session(vm *interp.Interpreter) (*Session, error) {
	sess, ok := vm.Context().Value(sessionKey{}).(*Session)
	if !ok || sess == nil {
		return nil, errors.New("runtime: no session in execution context")
	}
	return sess, nil
}

var (
	refUnaryToRef = &types.FunctionType{Params: []types.Type{types.TypeRef}, Returns: []types.Type{types.TypeRef}}
	refBinaryRef  = &types.FunctionType{Params: []types.Type{types.TypeRef, types.TypeRef}, Returns: []types.Type{types.TypeRef}}
	refToRef      = &types.FunctionType{Params: nil, Returns: []types.Type{types.TypeRef}}
	refToI32      = &types.FunctionType{Params: nil, Returns: []types.Type{types.TypeI32}}
	refUnaryToI32 = &types.FunctionType{Params: []types.Type{types.TypeRef}, Returns: []types.Type{types.TypeI32}}
)

// OpenFunc opens a scan over tbl into the session cursor. Arity 0, returns NULL.
func OpenFunc(tbl catalog.Table) *interp.HostFunction {
	return interp.NewHostFunction(refToRef, func(vm *interp.Interpreter, _ []types.Boxed) ([]types.Boxed, error) {
		sess, err := session(vm)
		if err != nil {
			return nil, err
		}
		cur, err := tbl.Scan(vm.Context())
		if err != nil {
			return nil, err
		}
		sess.cursor = cur
		return []types.Boxed{types.BoxedNull}, nil
	})
}

// NextFunc advances the session cursor, storing the next row. Arity 0, returns
// i32 1 if a row was read or 0 at end of stream.
func NextFunc() *interp.HostFunction {
	return interp.NewHostFunction(refToI32, func(vm *interp.Interpreter, _ []types.Boxed) ([]types.Boxed, error) {
		sess, err := session(vm)
		if err != nil {
			return nil, err
		}
		row, err := sess.cursor.Next()
		if errors.Is(err, io.EOF) {
			return []types.Boxed{types.BoxI32(0)}, nil
		}
		if err != nil {
			return nil, err
		}
		sess.row = row
		return []types.Boxed{types.BoxI32(1)}, nil
	})
}

// ColumnFunc resolves col from the session's current row. Arity 0, returns value.
func ColumnFunc(col *sqlparser.ColName) *interp.HostFunction {
	return interp.NewHostFunction(refToRef, func(vm *interp.Interpreter, _ []types.Boxed) ([]types.Boxed, error) {
		sess, err := session(vm)
		if err != nil {
			return nil, err
		}
		sv, found := sess.row.Get(col)
		var v value.Value
		if found && !sv.IsNull() {
			if v, err = value.FromSQL(sv); err != nil {
				return nil, err
			}
		}
		ref, err := Box(vm, v)
		if err != nil {
			return nil, err
		}
		return []types.Boxed{ref}, nil
	})
}

// CompareFunc builds a comparison over two SQL values yielding a boolean.
// NULL operands yield NULL (SQL three-valued logic). Arity 2.
func CompareFunc(op string) *interp.HostFunction {
	return HostFunc(2, func(args []value.Value) (value.Value, error) {
		if args[0] == nil || args[1] == nil {
			return nil, nil
		}
		c, err := value.Compare(args[0], args[1])
		if err != nil {
			return nil, err
		}
		return value.NewBool(satisfies(op, c)), nil
	})
}

func satisfies(op string, c int) bool {
	switch op {
	case sqlparser.EqualStr:
		return c == 0
	case sqlparser.NotEqualStr:
		return c != 0
	case sqlparser.LessThanStr:
		return c < 0
	case sqlparser.LessEqualStr:
		return c <= 0
	case sqlparser.GreaterThanStr:
		return c > 0
	case sqlparser.GreaterEqualStr:
		return c >= 0
	default:
		return false
	}
}

// AndFunc, OrFunc, NotFunc implement boolean logic over SQL truthiness.
func AndFunc() *interp.HostFunction {
	return HostFunc(2, func(args []value.Value) (value.Value, error) {
		return value.NewBool(value.ToBool(args[0]) && value.ToBool(args[1])), nil
	})
}

func OrFunc() *interp.HostFunction {
	return HostFunc(2, func(args []value.Value) (value.Value, error) {
		return value.NewBool(value.ToBool(args[0]) || value.ToBool(args[1])), nil
	})
}

func NotFunc() *interp.HostFunction {
	return HostFunc(1, func(args []value.Value) (value.Value, error) {
		return value.NewBool(!value.ToBool(args[0])), nil
	})
}

// TruthyFunc reduces a SQL value to a native boolean for conditional branches.
// Arity 1, returns i32 (0 or 1).
func TruthyFunc() *interp.HostFunction {
	return interp.NewHostFunction(refUnaryToI32, func(vm *interp.Interpreter, params []types.Boxed) ([]types.Boxed, error) {
		v, err := Unbox(vm, params[0])
		if err != nil {
			return nil, err
		}
		return []types.Boxed{types.BoxI32(boolToI32(value.ToBool(v)))}, nil
	})
}

func boolToI32(b bool) int32 {
	if b {
		return 1
	}
	return 0
}

// PushFunc appends a value to the session's current result row. Arity 1.
// Returns NULL.
func PushFunc() *interp.HostFunction {
	return interp.NewHostFunction(refUnaryToRef, func(vm *interp.Interpreter, params []types.Boxed) ([]types.Boxed, error) {
		sess, err := session(vm)
		if err != nil {
			return nil, err
		}
		v, err := Unbox(vm, params[0])
		if err != nil {
			return nil, err
		}
		if err := sess.result.Push(v); err != nil {
			return nil, err
		}
		return []types.Boxed{types.BoxedNull}, nil
	})
}

// CommitFunc finalizes the session's current result row. Arity 0. Returns NULL.
func CommitFunc() *interp.HostFunction {
	return interp.NewHostFunction(refToRef, func(vm *interp.Interpreter, _ []types.Boxed) ([]types.Boxed, error) {
		sess, err := session(vm)
		if err != nil {
			return nil, err
		}
		sess.result.Commit()
		return []types.Boxed{types.BoxedNull}, nil
	})
}

// PushKeyFunc appends an ORDER BY sort key to the current result row. Arity 1.
// Returns NULL.
func PushKeyFunc() *interp.HostFunction {
	return interp.NewHostFunction(refUnaryToRef, func(vm *interp.Interpreter, params []types.Boxed) ([]types.Boxed, error) {
		sess, err := session(vm)
		if err != nil {
			return nil, err
		}
		v, err := Unbox(vm, params[0])
		if err != nil {
			return nil, err
		}
		sess.result.PushKey(v)
		return []types.Boxed{types.BoxedNull}, nil
	})
}

// SortFunc orders the result by its pushed sort keys after the scan completes.
// Arity 0. Returns NULL.
func SortFunc(desc []bool) *interp.HostFunction {
	return interp.NewHostFunction(refToRef, func(vm *interp.Interpreter, _ []types.Boxed) ([]types.Boxed, error) {
		sess, err := session(vm)
		if err != nil {
			return nil, err
		}
		sess.result.Sort(desc)
		return []types.Boxed{types.BoxedNull}, nil
	})
}

// LimitFunc applies an OFFSET/row-count window to the result after the scan
// completes. Arity 0. Returns NULL.
func LimitFunc(offset, count int64) *interp.HostFunction {
	return interp.NewHostFunction(refToRef, func(vm *interp.Interpreter, _ []types.Boxed) ([]types.Boxed, error) {
		sess, err := session(vm)
		if err != nil {
			return nil, err
		}
		sess.result.Limit(offset, count)
		return []types.Boxed{types.BoxedNull}, nil
	})
}

// EmitStarFunc copies the session's current row into the result (SELECT *).
// Arity 0. Returns NULL.
func EmitStarFunc() *interp.HostFunction {
	return interp.NewHostFunction(refToRef, func(vm *interp.Interpreter, _ []types.Boxed) ([]types.Boxed, error) {
		sess, err := session(vm)
		if err != nil {
			return nil, err
		}
		sess.result.EmitRow(sess.row)
		return []types.Boxed{types.BoxedNull}, nil
	})
}
