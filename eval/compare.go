package eval

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type Equal struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*Equal)(nil)

func (e *Equal) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	left, err := e.Left.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	right, err := e.Right.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}

	cmp, err := Compare(left, right)
	if err != nil {
		return nil, err
	}
	return NewBool(cmp == 0), nil
}

func (e *Equal) String() string {
	return fmt.Sprintf("Equal(%s, %s)", e.Left.String(), e.Right.String())
}

type GreaterThan struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*GreaterThan)(nil)

func (e *GreaterThan) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	left, err := e.Left.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	right, err := e.Right.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}

	cmp, err := Compare(left, right)
	if err != nil {
		return nil, err
	}
	return NewBool(cmp > 0), nil
}

func (e *GreaterThan) String() string {
	return fmt.Sprintf("GreaterThan(%s, %s)", e.Left.String(), e.Right.String())
}

type GreaterThanOrEqual struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*GreaterThanOrEqual)(nil)

func (e *GreaterThanOrEqual) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	left, err := e.Left.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	right, err := e.Right.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}

	cmp, err := Compare(left, right)
	if err != nil {
		return nil, err
	}
	return NewBool(cmp >= 0), nil
}

func (e *GreaterThanOrEqual) String() string {
	return fmt.Sprintf("GreaterThanOrEqual(%s, %s)", e.Left.String(), e.Right.String())
}

type LessThan struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*LessThan)(nil)

func (e *LessThan) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	left, err := e.Left.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	right, err := e.Right.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}

	cmp, err := Compare(left, right)
	if err != nil {
		return nil, err
	}
	return NewBool(cmp < 0), nil
}

func (e *LessThan) String() string {
	return fmt.Sprintf("LessThan(%s, %s)", e.Left.String(), e.Right.String())
}

type LessThanOrEqual struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*LessThanOrEqual)(nil)

func (e *LessThanOrEqual) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	left, err := e.Left.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	right, err := e.Right.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}

	cmp, err := Compare(left, right)
	if err != nil {
		return nil, err
	}
	return NewBool(cmp <= 0), nil
}

func (e *LessThanOrEqual) String() string {
	return fmt.Sprintf("LessThanOrEqual(%s, %s)", e.Left.String(), e.Right.String())
}

type In struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*In)(nil)

func (e *In) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	left, err := e.Left.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	right, err := e.Right.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}

	if r, ok := right.(*Tuple); ok {
		for _, val := range r.Values() {
			if cmp, err := Compare(left, val); err != nil {
				return nil, err
			} else if cmp == 0 {
				return True, nil
			}
		}
	} else {
		if cmp, err := Compare(left, right); err != nil {
			return nil, err
		} else if cmp == 0 {
			return True, nil
		}
	}
	return False, nil
}

func (e *In) String() string {
	return fmt.Sprintf("In(%s, %s)", e.Left.String(), e.Right.String())
}

type Match struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*Match)(nil)

func (e *Match) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	left, err := e.Left.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	right, err := e.Right.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}

	var columns []string
	switch left := left.(type) {
	case *Tuple:
		for _, v := range left.Values() {
			col, err := ToString(v)
			if err != nil {
				return nil, err
			}
			columns = append(columns, col)
		}
	default:
		col, err := ToString(left)
		if err != nil {
			return nil, err
		}
		columns = append(columns, col)
	}

	pattern, err := ToString(right)
	if err != nil {
		return nil, err
	}

	for _, col := range columns {
		if strings.Contains(col, pattern) {
			return True, nil
		}
	}
	return False, nil
}

func (e *Match) String() string {
	return fmt.Sprintf("Match(%s, %s)", e.Left.String(), e.Right.String())
}

type Like struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*Like)(nil)

func (e *Like) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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

	pattern := "^" + regexp.QuoteMeta(rhs) + "$"
	pattern = strings.ReplaceAll(pattern, `%`, `.*`)
	pattern = strings.ReplaceAll(pattern, `_`, `.`)

	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	return NewBool(re.MatchString(lhs)), nil
}

func (e *Like) String() string {
	return fmt.Sprintf("Like(%s, %s)", e.Left.String(), e.Right.String())
}

type Regexp struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*Regexp)(nil)

func (e *Regexp) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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

	re, err := regexp.Compile(rhs)
	if err != nil {
		return nil, err
	}
	return NewBool(re.MatchString(lhs)), nil
}

func (e *Regexp) String() string {
	return fmt.Sprintf("Regexp(%s, %s)", e.Left.String(), e.Right.String())
}

type Uniform struct {
	Input Expr
}

var _ Expr = (*Uniform)(nil)

func (e *Uniform) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	val, err := e.Input.Eval(ctx, row, binds)
	if err != nil {
		return nil, err
	}
	switch val := val.(type) {
	case *Tuple:
		values := val.Values()
		for i := 0; i < len(values); i++ {
			for j := i + 1; j < len(values); j++ {
				if cmp, err := Compare(values[i], values[j]); err != nil {
					return nil, err
				} else if cmp != 0 {
					return False, nil
				}
			}
		}
		return True, nil
	default:
		return True, nil
	}
}

func (e *Uniform) String() string {
	return fmt.Sprintf("Uniform(%s)", e.Input.String())
}
