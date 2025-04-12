package plan

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

func (p *JSONExtract) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (*EvalResult, error) {
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

	lstr := ToString(lhs)
	rstr := ToString(rhs)

	var data any
	if err := json.Unmarshal([]byte(lstr), &data); err != nil {
		return nil, err
	}

	curr := reflect.ValueOf(data)
	rrune := []rune(rstr)

	quotation := false
	apostrophe := false
	j := 0
	for i := 0; i < len(rrune); i++ {
		ch := rrune[i]

		switch ch {
		case '.', '[', ']':
			if quotation || apostrophe {
				continue
			}
			if j < i-1 {
				key := string(rrune[j:i])
				if ch == ']' {
					if !strings.HasPrefix(key, "\"") || !strings.HasSuffix(key, "\"") {
						index, err := strconv.Atoi(key)
						if err != nil {
							return nil, err
						}
						if curr.Kind() == reflect.Slice && index < curr.Len() {
							curr = curr.Index(index)
						} else {
							return NULL, fmt.Errorf("index '%d' out of range", index)
						}
					} else {
						key = key[1 : len(key)-1]
					}
				}
				if curr.Kind() == reflect.Map {
					curr = curr.MapIndex(reflect.ValueOf(key))
					if !curr.IsValid() {
						return NULL, fmt.Errorf("key '%s' not found", key)
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

	if curr.IsValid() {
		typ, val, err := Marshal(curr.Interface())
		if err != nil {
			return nil, err
		}
		return &EvalResult{Type: typ, Value: val}, nil
	}
	return NULL, nil
}

func (p *JSONExtract) String() string {
	return fmt.Sprintf("JSONExtract(%s, %s)", p.Left.String(), p.Right.String())
}
