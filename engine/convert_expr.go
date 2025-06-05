package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type ConvertExpr struct {
	Input Expr
	Type  *sqlparser.ConvertType
}

var _ Expr = (*ConvertExpr)(nil)

func (e *ConvertExpr) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	input, err := e.Input.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	val, err := ToSQL(input, querypb.Type(querypb.Type_value[strings.ToUpper(e.Type.Type)]))
	if err != nil {
		return nil, err
	}
	return FromSQL(val)
}

func (e *ConvertExpr) String() string {
	return fmt.Sprintf("Convert(%s, %s)", e.Input.String(), sqlparser.String(e.Type))
}
