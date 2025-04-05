package task

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"

	"github.com/xwb1989/sqlparser"
)

func NewExpressionBuilder(builder Builder) Builder {
	return Build(func(node sqlparser.SQLNode) (Task, error) {
		switch n := node.(type) {
		case *sqlparser.AndExpr:
			a, err := builder.Build(n.Left)
			if err != nil {
				return nil, err
			}
			b, err := builder.Build(n.Right)
			if err != nil {
				return nil, err
			}
			return Run(func(ctx context.Context, value any) (any, error) {
				av, err := a.Run(ctx, value)
				if err != nil {
					return false, nil
				}
				ab, _ := av.(bool)
				if !ab {
					return false, nil
				}
				bv, err := b.Run(ctx, value)
				if err != nil {
					return false, nil
				}
				bb, _ := bv.(bool)
				return ab && bb, nil
			}), nil

		case *sqlparser.OrExpr:
			a, err := builder.Build(n.Left)
			if err != nil {
				return nil, err
			}
			b, err := builder.Build(n.Right)
			if err != nil {
				return nil, err
			}
			return Run(func(ctx context.Context, value any) (any, error) {
				av, err := a.Run(ctx, value)
				if err != nil {
					return false, nil
				}
				ab, _ := av.(bool)
				if ab {
					return true, nil
				}
				bv, err := b.Run(ctx, value)
				if err != nil {
					return false, nil
				}
				bb, _ := bv.(bool)
				return bb, nil
			}), nil

		case *sqlparser.NotExpr:
			t, err := builder.Build(n.Expr)
			if err != nil {
				return nil, err
			}
			return Run(func(ctx context.Context, value any) (any, error) {
				v, err := t.Run(ctx, value)
				if err != nil {
					return nil, err
				}
				b, _ := v.(bool)
				return !b, nil
			}), nil

		case *sqlparser.ParenExpr:
			return builder.Build(n.Expr)

		case *sqlparser.ComparisonExpr:
			left, err := builder.Build(n.Left)
			if err != nil {
				return nil, err
			}
			right, err := builder.Build(n.Right)
			if err != nil {
				return nil, err
			}
			return Run(func(ctx context.Context, value any) (any, error) {
				lv, err := left.Run(ctx, value)
				if err != nil {
					return false, nil
				}
				rv, err := right.Run(ctx, value)
				if err != nil {
					return false, nil
				}

				switch n.Operator {
				case sqlparser.EqualStr:
					return reflect.DeepEqual(lv, rv), nil
				case sqlparser.NotEqualStr:
					return !reflect.DeepEqual(lv, rv), nil
				}

				cmp, err := compare(lv, rv)
				if err != nil {
					return nil, driver.ErrSkip
				}

				switch n.Operator {
				case sqlparser.LessThanStr:
					return cmp < 0, nil
				case sqlparser.GreaterThanStr:
					return cmp > 0, nil
				case sqlparser.LessEqualStr:
					return cmp <= 0, nil
				case sqlparser.GreaterEqualStr:
					return cmp >= 0, nil
				}
				return nil, fmt.Errorf("sqlbridge: unsupported operator %q", n.Operator)
			}), nil

		case *sqlparser.RangeCond:
			left, err := builder.Build(n.Left)
			if err != nil {
				return nil, err
			}
			from, err := builder.Build(n.From)
			if err != nil {
				return nil, err
			}
			to, err := builder.Build(n.To)
			if err != nil {
				return nil, err
			}

			return Run(func(ctx context.Context, value any) (any, error) {
				lv, err := left.Run(ctx, value)
				if err != nil {
					return false, nil
				}
				fv, err := from.Run(ctx, value)
				if err != nil {
					return false, nil
				}
				tv, err := to.Run(ctx, value)
				if err != nil {
					return false, nil
				}

				fc, err := compare(lv, fv)
				if err != nil {
					return nil, err
				}

				tc, err := compare(lv, tv)
				if err != nil {
					return nil, err
				}

				inRange := fc >= 0 && tc <= 0
				if n.Operator == sqlparser.NotBetweenStr {
					return !inRange, nil
				}
				return inRange, nil
			}), nil

		case *sqlparser.IsExpr:
			expr, err := builder.Build(n.Expr)
			if err != nil {
				return nil, err
			}

			return Run(func(ctx context.Context, value any) (any, error) {
				v, err := expr.Run(ctx, value)
				if err != nil {
					return nil, err
				}

				switch n.Operator {
				case sqlparser.IsNullStr:
					return v == nil, nil
				case sqlparser.IsNotNullStr:
					return v != nil, nil
				case sqlparser.IsTrueStr:
					b, ok := v.(bool)
					return ok && b, nil
				case sqlparser.IsNotTrueStr:
					b, ok := v.(bool)
					return !ok || !b, nil
				case sqlparser.IsFalseStr:
					b, ok := v.(bool)
					return ok && !b, nil
				case sqlparser.IsNotFalseStr:
					b, ok := v.(bool)
					return !ok || b, nil
				default:
					return nil, fmt.Errorf("sqlbridge: unsupported operator %q", n.Operator)
				}
			}), nil

		case *sqlparser.ExistsExpr:
			t, err := builder.Build(n.Subquery)
			if err != nil {
				return nil, err
			}
			return Run(func(ctx context.Context, value any) (any, error) {
				val, err := t.Run(ctx, value)
				if err != nil {
					return false, nil
				}
				if v, ok := val.(driver.Rows); ok {
					values := make([]driver.Value, len(v.Columns()))
					if err := v.Next(values); err != nil {
						if errors.Is(err, io.EOF) {
							return false, nil
						}
						return false, err
					} else {
						return true, nil
					}
				}
				return 0, fmt.Errorf("sqlbridge: unsupported types %T", val)
			}), nil

		case *sqlparser.SQLVal:
			switch n.Type {
			case sqlparser.StrVal:
				return Run(func(_ context.Context, _ any) (any, error) {
					return string(n.Val), nil
				}), nil
			case sqlparser.IntVal:
				return Run(func(_ context.Context, _ any) (any, error) {
					v, err := strconv.ParseInt(string(n.Val), 10, 64)
					if err != nil {
						return nil, err
					}
					return v, nil
				}), nil
			case sqlparser.FloatVal:
				return Run(func(_ context.Context, _ any) (any, error) {
					v, err := strconv.ParseFloat(string(n.Val), 64)
					if err != nil {
						return nil, err
					}
					return v, nil
				}), nil
			case sqlparser.HexVal:
				return Run(func(_ context.Context, _ any) (any, error) {
					v, err := hex.DecodeString(string(n.Val))
					if err != nil {
						return nil, err
					}
					return v, nil
				}), nil
			case sqlparser.ValArg:
				return Run(func(_ context.Context, value any) (any, error) {
					record, ok := value.(map[string]driver.Value)
					if !ok {
						return nil, driver.ErrSkip
					}
					return record[string(n.Val)], nil
				}), nil
			case sqlparser.BitVal:
				return Run(func(_ context.Context, value any) (any, error) {
					v, err := strconv.ParseInt(string(n.Val), 2, 64)
					if err != nil {
						return nil, err
					}
					return v, nil
				}), nil
			default:
			}

		case *sqlparser.NullVal:
			return Run(func(_ context.Context, _ any) (any, error) {
				return nil, nil
			}), nil

		case sqlparser.BoolVal:
			return Run(func(_ context.Context, _ any) (any, error) {
				return bool(n), nil
			}), nil

		case *sqlparser.ColName:
			return Run(func(ctx context.Context, value any) (any, error) {
				record, ok := value.(map[*sqlparser.ColName]driver.Value)
				if !ok {
					return nil, driver.ErrSkip
				}
				for k, v := range record {
					if k.Name.Equal(n.Name) && (n.Qualifier == (sqlparser.TableName{}) || k.Qualifier == n.Qualifier) {
						return v, nil
					}
				}
				return nil, nil
			}), nil

		case sqlparser.ValTuple:
			tasks := make([]Task, len(n))
			for i, expr := range n {
				t, err := builder.Build(expr)
				if err != nil {
					return nil, err
				}
				tasks[i] = t
			}
			return Run(func(ctx context.Context, value any) (any, error) {
				values := make([]any, 0, len(tasks))
				for _, t := range tasks {
					v, err := t.Run(ctx, value)
					if err != nil {
						return nil, err
					}
					values = append(values, v)
				}
				return values, nil
			}), nil

		case sqlparser.ListArg:
		case *sqlparser.BinaryExpr:
		case *sqlparser.UnaryExpr:
		case *sqlparser.IntervalExpr:
		case *sqlparser.CollateExpr:
		case *sqlparser.FuncExpr:
		case *sqlparser.CaseExpr:
		case *sqlparser.ValuesFuncExpr:
		case *sqlparser.ConvertExpr:
		case *sqlparser.SubstrExpr:
		case *sqlparser.ConvertUsingExpr:
		case *sqlparser.MatchExpr:
		case *sqlparser.GroupConcatExpr:
		case *sqlparser.Default:
		}
		return nil, driver.ErrSkip
	})
}

func compare(a, b any) (int, error) {
	toFloat := func(v any) (float64, bool) {
		switch x := v.(type) {
		case int:
			return float64(x), true
		case int32:
			return float64(x), true
		case int64:
			return float64(x), true
		case uint:
			return float64(x), true
		case uint32:
			return float64(x), true
		case uint64:
			return float64(x), true
		case float32:
			return float64(x), true
		case float64:
			return x, true
		}
		return 0, false
	}

	if lf, lok := toFloat(a); lok {
		if rf, rok := toFloat(b); rok {
			switch {
			case lf < rf:
				return -1, nil
			case lf > rf:
				return 1, nil
			default:
				return 0, nil
			}
		}
	}

	if ls, ok := a.(string); ok {
		if rs, ok := b.(string); ok {
			return strings.Compare(ls, rs), nil
		}
	}

	if lb, ok := a.([]byte); ok {
		if rb, ok := b.([]byte); ok {
			return bytes.Compare(lb, rb), nil
		}
	}

	return 0, fmt.Errorf("sqlbridge: unsupported compare types %T and %T", a, b)
}
