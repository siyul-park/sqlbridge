package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type JSONExtractExpr struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*JSONExtractExpr)(nil)

func (e *JSONExtractExpr) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	left, err := e.Left.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	right, err := e.Right.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}

	lhs, err := ToString(left)
	if err != nil {
		return nil, err
	}
	rhs, err := ToString(right)
	if err != nil {
		return nil, err
	}

	if !strings.HasPrefix(rhs, "$") {
		return nil, fmt.Errorf("invalid JSON path: must start with $")
	}
	path := rhs[1:]

	var data any
	if err := json.Unmarshal([]byte(lhs), &data); err != nil {
		return nil, err
	}
	curr := reflect.ValueOf(data)

	token := ""
	bracket, quote := false, false
	for i := 0; i <= len(path); i++ {
		var ch byte
		if i < len(path) {
			ch = path[i]
		}

		flush := i == len(path) || (!quote && !bracket && (ch == '.' || ch == '[')) || (bracket && !quote && ch == ']')

		if i < len(path) {
			if bracket {
				if ch == '"' {
					quote = !quote
				} else if ch == ']' && !quote {
					bracket = false
				} else {
					token += string(ch)
				}
			} else if ch == '[' {
				bracket = true
			} else if ch != '.' {
				token += string(ch)
			}
		}

		if flush && token != "" {
			if curr.Kind() == reflect.Map {
				key := token
				v := curr.MapIndex(reflect.ValueOf(key))
				if !v.IsValid() {
					return nil, nil
				}
				curr = reflect.ValueOf(v.Interface())
			} else if curr.Kind() == reflect.Slice {
				idx, err := strconv.Atoi(token)
				if err != nil || idx < 0 || idx >= curr.Len() {
					return nil, nil
				}
				curr = reflect.ValueOf(curr.Index(idx).Interface())
			} else {
				return nil, nil
			}
			token = ""
		}
	}

	if !curr.IsValid() {
		return nil, nil
	}
	return NewValue(curr.Interface()), nil
}

func (e *JSONExtractExpr) Walk(f func(Expr) (bool, error)) (bool, error) {
	if cont, err := f(e); !cont || err != nil {
		return cont, err
	}
	if cont, err := e.Left.Walk(f); !cont || err != nil {
		return cont, err
	}
	return e.Right.Walk(f)
}

func (e *JSONExtractExpr) Copy() Expr {
	return &JSONExtractExpr{
		Left:  e.Left.Copy(),
		Right: e.Right.Copy(),
	}
}

func (e *JSONExtractExpr) String() string {
	return fmt.Sprintf("JSONExtract(%s, %s)", e.Left.String(), e.Right.String())
}
