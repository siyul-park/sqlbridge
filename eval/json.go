package eval

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

type JSONExtract struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*JSONExtract)(nil)

func (e *JSONExtract) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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

	var data any
	if err := json.Unmarshal([]byte(lhs), &data); err != nil {
		return nil, err
	}

	key := []rune(rhs)
	curr := reflect.ValueOf(data)

	quotation := false
	apostrophe := false

	j := 0
	for i := 0; i < len(key); i++ {
		ch := key[i]

		switch ch {
		case '.', '[', ']':
			if quotation || apostrophe {
				continue
			}
			if j < i-1 {
				key := string(key[j:i])
				if ch == ']' {
					if !strings.HasPrefix(key, "\"") || !strings.HasSuffix(key, "\"") {
						index, err := strconv.Atoi(key)
						if err != nil {
							return nil, err
						}
						if curr.Kind() == reflect.Slice && index < curr.Len() {
							curr = curr.Index(index)
						} else {
							return nil, fmt.Errorf("index '%d' out of range", index)
						}
					} else {
						key = key[1 : len(key)-1]
					}
				}
				if curr.Kind() == reflect.Map {
					curr = curr.MapIndex(reflect.ValueOf(key))
					if !curr.IsValid() {
						return nil, fmt.Errorf("rhs '%s' not found", key)
					}
				}
			}
			j = i + 1
		case '"':
			if !apostrophe {
				quotation = !quotation
			}
		case '\'':
			if !quotation {
				apostrophe = !apostrophe
			}
		}
	}
	if !curr.IsValid() {
		return nil, nil
	}
	return NewValue(curr.Interface()), nil
}

func (e *JSONExtract) String() string {
	return fmt.Sprintf("JSONExtract(%s, %s)", e.Left.String(), e.Right.String())
}
