package eval

import (
	"context"
	"fmt"
	"strings"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type Match struct {
	Input   Expr
	Columns []Expr
}

var _ Expr = (*Match)(nil)

func (e *Match) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	var columns []string
	for _, expr := range e.Columns {
		val, err := expr.Eval(ctx, row, binds)
		if err != nil {
			return nil, err
		}

		var vals []Value
		switch val := val.(type) {
		case *Tuple:
			vals = append(vals, val.Values()...)
		default:
			vals = append(vals, val)
		}

		for _, v := range vals {
			col, err := ToString(v)
			if err != nil {
				return nil, err
			}
			columns = append(columns, col)
		}
	}

	expr, err := e.Input.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	str, err := ToString(expr)
	if err != nil {
		return nil, err
	}

	for _, col := range columns {
		if strings.Contains(col, str) {
			return True, nil
		}
	}
	return False, nil
}

func (e *Match) String() string {
	var columns []string
	for _, col := range e.Columns {
		columns = append(columns, col.String())
	}
	return fmt.Sprintf("Match(%s, %s)", strings.Join(columns, ", "), e.Input.String())
}
