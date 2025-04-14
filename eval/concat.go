package eval

import (
	"context"
	"fmt"
	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
	"strings"
)

type Concat struct {
	Input     Expr
	Separator string
}

var _ Expr = (*Concat)(nil)

func (e *Concat) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	input, err := e.Input.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	val, ok := input.(*Tuple)
	if !ok {
		return val, nil
	}

	var vals []string
	for _, val := range val.Values() {
		var tokens []string
		switch v := val.(type) {
		case *Tuple:
			for _, t := range v.Values() {
				s, err := ToString(t)
				if err != nil {
					return nil, err
				}
				tokens = append(tokens, s)
			}
		default:
			s, err := ToString(v)
			if err != nil {
				return nil, err
			}
			tokens = append(tokens, s)
		}
		vals = append(vals, strings.Join(tokens, ""))
	}
	return NewString(strings.Join(vals, e.Separator)), nil
}

func (e *Concat) String() string {
	return fmt.Sprintf("Concat(%s, %s)", e.Input.String(), e.Separator)
}
