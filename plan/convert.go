package plan

import (
	"context"
	"fmt"
	"strings"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type Convert struct {
	Input Expr
	Type  *sqlparser.ConvertType
}

var _ Expr = (*Convert)(nil)

func (p *Convert) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (*EvalResult, error) {
	input, err := p.Input.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}

	val, err := Unmarshal(input.Type, input.Value)
	if err != nil {
		return nil, err
	}

	switch typ := querypb.Type(querypb.Type_value[strings.ToUpper(p.Type.Type)]); typ {
	case querypb.Type_NULL_TYPE:
		return NULL, nil
	case querypb.Type_INT8, querypb.Type_INT16, querypb.Type_INT24, querypb.Type_INT32, querypb.Type_INT64:
		_, data, err := Marshal(ToInt(val))
		if err != nil {
			return nil, err
		}
		return &EvalResult{Type: typ, Value: data}, nil
	case querypb.Type_UINT8, querypb.Type_UINT16, querypb.Type_UINT24, querypb.Type_UINT32, querypb.Type_UINT64:
		_, data, err := Marshal(uint(ToInt(val)))
		if err != nil {
			return nil, err
		}
		return &EvalResult{Type: typ, Value: data}, nil
	case querypb.Type_FLOAT32:
		_, data, err := Marshal(float32(ToFloat(val)))
		if err != nil {
			return nil, err
		}
		return &EvalResult{Type: typ, Value: data}, nil
	case querypb.Type_FLOAT64:
		_, data, err := Marshal(ToFloat(val))
		if err != nil {
			return nil, err
		}
		return &EvalResult{Type: typ, Value: data}, nil
	case querypb.Type_TEXT, querypb.Type_VARCHAR, querypb.Type_CHAR, querypb.Type_ENUM, querypb.Type_SET:
		_, data, err := Marshal(ToString(val))
		if err != nil {
			return nil, err
		}
		return &EvalResult{Type: typ, Value: data}, nil
	default:
		v, err := Unmarshal(typ, []byte(ToString(val)))
		if err != nil {
			return nil, err
		}
		_, data, err := Marshal(v)
		if err != nil {
			return nil, err
		}
		return &EvalResult{Type: typ, Value: data}, nil
	}
}

func (p *Convert) String() string {
	return fmt.Sprintf("Convert(%s, %s)", p.Input.String(), sqlparser.String(p.Type))
}
