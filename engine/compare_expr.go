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

func (e *EqualExpr) Walk(f func(Expr) (bool, error)) (bool, error) {
	if cont, err := f(e); !cont || err != nil {
		return cont, err
	}
	if cont, err := e.Left.Walk(f); !cont || err != nil {
		return cont, err
	}
	return e.Right.Walk(f)
}

func (e *EqualExpr) Copy() Expr {
	return &EqualExpr{
		Left:  e.Left.Copy(),
		Right: e.Right.Copy(),
	}
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

func (e *GreaterThanExpr) Walk(f func(Expr) (bool, error)) (bool, error) {
	if cont, err := f(e); !cont || err != nil {
		return cont, err
	}
	if cont, err := e.Left.Walk(f); !cont || err != nil {
		return cont, err
	}
	return e.Right.Walk(f)
}

func (e *GreaterThanExpr) Copy() Expr {
	return &GreaterThanExpr{
		Left:  e.Left.Copy(),
		Right: e.Right.Copy(),
	}
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

func (e *GreaterThanOrEqualExpr) Walk(f func(Expr) (bool, error)) (bool, error) {
	if cont, err := f(e); !cont || err != nil {
		return cont, err
	}
	if cont, err := e.Left.Walk(f); !cont || err != nil {
		return cont, err
	}
	return e.Right.Walk(f)
}

func (e *GreaterThanOrEqualExpr) Copy() Expr {
	return &GreaterThanOrEqualExpr{
		Left:  e.Left.Copy(),
		Right: e.Right.Copy(),
	}
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

func (e *LessThanExpr) Walk(f func(Expr) (bool, error)) (bool, error) {
	if cont, err := f(e); !cont || err != nil {
		return cont, err
	}
	if cont, err := e.Left.Walk(f); !cont || err != nil {
		return cont, err
	}
	return e.Right.Walk(f)
}

func (e *LessThanExpr) Copy() Expr {
	return &LessThanExpr{
		Left:  e.Left.Copy(),
		Right: e.Right.Copy(),
	}
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

func (e *LessThanOrEqualExpr) Walk(f func(Expr) (bool, error)) (bool, error) {
	if cont, err := f(e); !cont || err != nil {
		return cont, err
	}
	if cont, err := e.Left.Walk(f); !cont || err != nil {
		return cont, err
	}
	return e.Right.Walk(f)
}

func (e *LessThanOrEqualExpr) Copy() Expr {
	return &LessThanOrEqualExpr{
		Left:  e.Left.Copy(),
		Right: e.Right.Copy(),
	}
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

func (e *InExpr) Walk(f func(Expr) (bool, error)) (bool, error) {
	if cont, err := f(e); !cont || err != nil {
		return cont, err
	}
	if cont, err := e.Left.Walk(f); !cont || err != nil {
		return cont, err
	}
	return e.Right.Walk(f)
}

func (e *InExpr) Copy() Expr {
	return &InExpr{
		Left:  e.Left.Copy(),
		Right: e.Right.Copy(),
	}
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

func (e *MatchExpr) Walk(f func(Expr) (bool, error)) (bool, error) {
	if cont, err := f(e); !cont || err != nil {
		return cont, err
	}
	if cont, err := e.Left.Walk(f); !cont || err != nil {
		return cont, err
	}
	return e.Right.Walk(f)
}

func (e *MatchExpr) Copy() Expr {
	return &MatchExpr{
		Left:  e.Left.Copy(),
		Right: e.Right.Copy(),
	}
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

	parts := strings.Split(rhs, "%")
	for i, part := range parts {
		tokens := strings.Split(part, "_")
		for j, tk := range tokens {
			tokens[j] = regexp.QuoteMeta(tk)
		}
		parts[i] = strings.Join(tokens, ".")
	}

	pattern := "^" + strings.Join(parts, ".*") + "$"

	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	return NewBool(re.MatchString(lhs)), nil
}

func (e *LikeExpr) Walk(f func(Expr) (bool, error)) (bool, error) {
	if cont, err := f(e); !cont || err != nil {
		return cont, err
	}
	if cont, err := e.Left.Walk(f); !cont || err != nil {
		return cont, err
	}
	return e.Right.Walk(f)
}

func (e *LikeExpr) Copy() Expr {
	return &LikeExpr{
		Left:  e.Left.Copy(),
		Right: e.Right.Copy(),
	}
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

func (e *RegexpExpr) Walk(f func(Expr) (bool, error)) (bool, error) {
	if cont, err := f(e); !cont || err != nil {
		return cont, err
	}
	if cont, err := e.Left.Walk(f); !cont || err != nil {
		return cont, err
	}
	return e.Right.Walk(f)
}

func (e *RegexpExpr) Copy() Expr {
	return &RegexpExpr{
		Left:  e.Left.Copy(),
		Right: e.Right.Copy(),
	}
}

func (e *RegexpExpr) String() string {
	return fmt.Sprintf("Regexp(%s, %s)", e.Left.String(), e.Right.String())
}

type IdenticalExpr struct {
	Input Expr
}

var _ Expr = (*IdenticalExpr)(nil)

func (e *IdenticalExpr) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
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

func (e *IdenticalExpr) Walk(f func(Expr) (bool, error)) (bool, error) {
	if cont, err := f(e); !cont || err != nil {
		return cont, err
	}
	return e.Input.Walk(f)
}

func (e *IdenticalExpr) Copy() Expr {
	return &IdenticalExpr{
		Input: e.Input.Copy(),
	}
}

func (e *IdenticalExpr) String() string {
	return fmt.Sprintf("Identical(%s)", e.Input.String())
}
