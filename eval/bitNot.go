package eval

import (
	"context"
	"fmt"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type BitNot struct {
	Input Expr
}

var _ Expr = (*BitNot)(nil)

func (e *BitNot) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	input, err := e.Input.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}

	switch l := input.(type) {
	case *Int64:
		return NewInt64(^l.Int()), nil
	case *Uint64:
		return NewUint64(^l.Uint()), nil
	default:
		return nil, fmt.Errorf("cannot apply bitwise NOT to %T", input)
	}
}

func (e *BitNot) String() string {
	return fmt.Sprintf("BitNot(%s)", e.Input)
}
