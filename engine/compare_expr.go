package engine

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type EqualExpr struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*EqualExpr)(nil)

func (e *EqualExpr) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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

func (e *EqualExpr) String() string {
	return fmt.Sprintf("Equal(%s, %s)", e.Left.String(), e.Right.String())
}

type GreaterThanExpr struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*GreaterThanExpr)(nil)

func (e *GreaterThanExpr) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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

func (e *GreaterThanExpr) String() string {
	return fmt.Sprintf("GreaterThan(%s, %s)", e.Left.String(), e.Right.String())
}

type GreaterThanOrEqualExpr struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*GreaterThanOrEqualExpr)(nil)

func (e *GreaterThanOrEqualExpr) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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

func (e *GreaterThanOrEqualExpr) String() string {
	return fmt.Sprintf("GreaterThanOrEqual(%s, %s)", e.Left.String(), e.Right.String())
}

type LessThanExpr struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*LessThanExpr)(nil)

func (e *LessThanExpr) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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

func (e *LessThanExpr) String() string {
	return fmt.Sprintf("LessThan(%s, %s)", e.Left.String(), e.Right.String())
}

type LessThanOrEqualExpr struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*LessThanOrEqualExpr)(nil)

func (e *LessThanOrEqualExpr) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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

func (e *LessThanOrEqualExpr) String() string {
	return fmt.Sprintf("LessThanOrEqual(%s, %s)", e.Left.String(), e.Right.String())
}

type InExpr struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*InExpr)(nil)

func (e *InExpr) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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

func (e *InExpr) String() string {
	return fmt.Sprintf("In(%s, %s)", e.Left.String(), e.Right.String())
}

type MatchExpr struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*MatchExpr)(nil)

func (e *MatchExpr) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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

func (e *MatchExpr) String() string {
	return fmt.Sprintf("Match(%s, %s)", e.Left.String(), e.Right.String())
}

type LikeExpr struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*LikeExpr)(nil)

func (e *LikeExpr) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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

func (e *LikeExpr) String() string {
	return fmt.Sprintf("Like(%s, %s)", e.Left.String(), e.Right.String())
}

type RegexpExpr struct {
	Left  Expr
	Right Expr
}

var _ Expr = (*RegexpExpr)(nil)

func (e *RegexpExpr) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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

func (e *RegexpExpr) String() string {
	return fmt.Sprintf("Regexp(%s, %s)", e.Left.String(), e.Right.String())
}

type UniformExpr struct {
	Input Expr
}

var _ Expr = (*UniformExpr)(nil)

func (e *UniformExpr) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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

func (e *UniformExpr) String() string {
	return fmt.Sprintf("Uniform(%s)", e.Input.String())
}
