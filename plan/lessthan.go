package plan

import (
	"bytes"
	"context"
	"fmt"
	"reflect"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type LessThan struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*LessThan)(nil)

func (p *LessThan) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (*schema.Value, error) {
	left, err := p.Left.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	lhs, err := Unmarshal(left.Type, left.Value)
	if err != nil {
		return nil, err
	}

	right, err := p.Right.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	rhs, err := Unmarshal(right.Type, right.Value)
	if err != nil {
		return nil, err
	}

	lhs, rhs = Promote(lhs, rhs)
	if reflect.TypeOf(lhs) != reflect.TypeOf(rhs) {
		return nil, fmt.Errorf("type mismatch: %v vs %v", reflect.TypeOf(lhs), reflect.TypeOf(rhs))
	}

	lval := reflect.ValueOf(lhs)
	rval := reflect.ValueOf(rhs)

	switch lval.Kind() {
	case reflect.Bool:
		if lval.Bool() && !rval.Bool() {
			return schema.True, nil
		}
		return schema.False, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if lval.Int() < rval.Int() {
			return schema.True, nil
		}
		return schema.False, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if lval.Uint() < rval.Uint() {
			return schema.True, nil
		}
		return schema.False, nil
	case reflect.Float32, reflect.Float64:
		if lval.Float() < rval.Float() {
			return schema.True, nil
		}
		return schema.False, nil
	case reflect.String:
		if lval.String() < rval.String() {
			return schema.True, nil
		}
		return schema.False, nil
	case reflect.Slice:
		if lval.Type().Elem().Kind() == reflect.Uint8 {
			if bytes.Compare(lval.Bytes(), rval.Bytes()) < 0 {
				return schema.True, nil
			}
		}
	default:
		return schema.Null, fmt.Errorf("unsupported type: %v", reflect.TypeOf(lhs))
	}

	return schema.False, nil
}

func (p *LessThan) String() string {
	return fmt.Sprintf("LessThan(%s, %s)", p.Left.String(), p.Right.String())
}
