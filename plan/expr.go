package plan

import (
	"context"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type Expr interface {
	Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (*EvalResult, error)
	String() string
}

type EvalResult struct {
	Type   querypb.Type
	Value  []byte
	Values []*EvalResult
}

var NULL = &EvalResult{Type: querypb.Type_NULL_TYPE}
var TRUE = &EvalResult{Type: querypb.Type_INT64, Value: []byte("1")}
var FALSE = &EvalResult{Type: querypb.Type_INT64, Value: []byte("0")}
