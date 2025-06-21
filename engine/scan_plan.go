package engine

import (
	"context"
	"strings"

	"github.com/xwb1989/sqlparser/dependency/sqltypes"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type ScanPlan struct {
	Catalog schema.Catalog
	Table   sqlparser.TableName
	Expr    Expr
}

var _ Plan = (*ScanPlan)(nil)

func (p *ScanPlan) Run(ctx context.Context, bindVars map[string]*querypb.BindVariable) (schema.Cursor, error) {
	table, err := p.Catalog.Table(p.Table.Name.CompliantName())
	if err != nil {
		return nil, err
	}
	indexes, err := table.Indexes(ctx)
	if err != nil {
		return nil, err
	}

	var hints []schema.ScanHint
	for _, idx := range indexes {
		hint, err := p.buildScanHint(ctx, idx, p.Expr, bindVars)
		if err != nil {
			return nil, err
		}

		ok := false
		for i, r := range hint.Ranges {
			if r.Min != nil || r.Max != nil {
				ok = true
			} else {
				for i = i + 1; i < len(hint.Ranges); i++ {
					if hint.Ranges[i].Min != nil || hint.Ranges[i].Max != nil {
						ok = false
						break
					}
				}
				break
			}
		}
		if !ok {
			continue
		}

		hints = append(hints, hint)
	}

	return table.Scan(ctx, hints...)
}

func (p *ScanPlan) String() string {
	var b strings.Builder
	b.WriteString("ScanPlan(")
	b.WriteString(sqlparser.String(p.Table))
	if p.Expr != nil {
		b.WriteString(", ")
		b.WriteString(p.Expr.String())
	}
	b.WriteString(")")
	return b.String()
}

func (p *ScanPlan) buildScanHint(ctx context.Context, index schema.Index, expr Expr, bindVars map[string]*querypb.BindVariable) (schema.ScanHint, error) {
	hint := schema.ScanHint{
		Index:  index.Name,
		Ranges: make([]schema.Range, len(index.Columns)),
	}

	switch e := expr.(type) {
	case *AndExpr:
		left, err := p.buildScanHint(ctx, index, e.Left, bindVars)
		if err != nil {
			return schema.ScanHint{}, err
		}
		right, err := p.buildScanHint(ctx, index, e.Right, bindVars)
		if err != nil {
			return schema.ScanHint{}, err
		}

		for i := 0; i < len(hint.Ranges); i++ {
			var leftRange, rightRange schema.Range
			if i < len(left.Ranges) {
				leftRange = left.Ranges[i]
			}
			if i < len(right.Ranges) {
				rightRange = right.Ranges[i]
			}

			if leftRange.Min == nil {
				hint.Ranges[i].Min = rightRange.Min
			} else if rightRange.Min == nil {
				hint.Ranges[i].Min = leftRange.Min
			} else {
				lhs, err := FromSQL(*leftRange.Min)
				if err != nil {
					return schema.ScanHint{}, err
				}
				rhs, err := FromSQL(*rightRange.Min)
				if err != nil {
					return schema.ScanHint{}, err
				}

				if cmp, err := Compare(lhs, rhs); err != nil {
					return schema.ScanHint{}, err
				} else if cmp > 0 {
					hint.Ranges[i].Min = leftRange.Min
				} else {
					hint.Ranges[i].Min = rightRange.Min
				}
			}

			if leftRange.Max == nil {
				hint.Ranges[i].Max = rightRange.Max
			} else if rightRange.Max == nil {
				hint.Ranges[i].Max = leftRange.Max
			} else {
				lhs, err := FromSQL(*leftRange.Max)
				if err != nil {
					return schema.ScanHint{}, err
				}
				rhs, err := FromSQL(*rightRange.Max)
				if err != nil {
					return schema.ScanHint{}, err
				}

				if cmp, err := Compare(lhs, rhs); err != nil {
					return schema.ScanHint{}, err
				} else if cmp < 0 {
					hint.Ranges[i].Max = leftRange.Max
				} else {
					hint.Ranges[i].Max = rightRange.Max
				}
			}
		}
		return hint, nil

	case *EqualExpr, *GreaterThanExpr, *GreaterThanOrEqualExpr, *LessThanExpr, *LessThanOrEqualExpr:
		var colExpr *ColumnExpr
		var valExpr Expr
		switch e := e.(type) {
		case *EqualExpr:
			colExpr, valExpr = p.operands(e.Left, e.Right, index)
		case *GreaterThanExpr:
			colExpr, valExpr = p.operands(e.Left, e.Right, index)
		case *GreaterThanOrEqualExpr:
			colExpr, valExpr = p.operands(e.Left, e.Right, index)
		case *LessThanExpr:
			colExpr, valExpr = p.operands(e.Left, e.Right, index)
		case *LessThanOrEqualExpr:
			colExpr, valExpr = p.operands(e.Left, e.Right, index)
		}
		if colExpr == nil || valExpr == nil {
			return schema.ScanHint{}, nil
		}

		val, err := valExpr.Eval(ctx, schema.Row{}, bindVars)
		if err != nil {
			return schema.ScanHint{}, err
		}
		sqlVal, err := ToSQL(val, val.Type())
		if err != nil {
			return schema.ScanHint{}, err
		}

		offset := -1
		for i, col := range index.Columns {
			if col.Equal(colExpr.Value) {
				offset = i
				break
			}
		}
		if offset == -1 {
			return schema.ScanHint{}, nil
		}

		rng := schema.Range{}

		switch e.(type) {
		case *EqualExpr:
			rng.Min = &sqlVal
			rng.Max = &sqlVal
		case *GreaterThanExpr:
			rng.Min = &sqlVal
		case *GreaterThanOrEqualExpr:
			rng.Min = &sqlVal
		case *LessThanExpr:
			rng.Max = &sqlVal
		case *LessThanOrEqualExpr:
			rng.Max = &sqlVal
		}

		hint.Ranges[offset] = rng
		return hint, nil

	case *InExpr:
		colExpr, ok := p.colName(e.Left)
		if !ok || !p.isIndexable(index, colExpr) {
			return schema.ScanHint{}, nil
		}

		val, err := e.Right.Eval(ctx, schema.Row{}, bindVars)
		if err != nil {
			return schema.ScanHint{}, err
		}

		tuple, ok := val.(*Tuple)
		if !ok || len(tuple.Values()) == 0 {
			return schema.ScanHint{}, nil
		}

		offset := -1
		for i, col := range index.Columns {
			if col.Equal(colExpr.Value) {
				offset = i
				break
			}
		}
		if offset == -1 {
			return schema.ScanHint{}, nil
		}

		var minVal, maxVal *sqltypes.Value

		for _, val := range tuple.Values() {
			sqlVal, err := ToSQL(val, val.Type())
			if err != nil {
				return schema.ScanHint{}, err
			}

			if minVal == nil && maxVal == nil {
				minVal, maxVal = &sqlVal, &sqlVal
				continue
			}

			currMin, err := FromSQL(*minVal)
			if err != nil {
				return schema.ScanHint{}, err
			}
			currMax, err := FromSQL(*maxVal)
			if err != nil {
				return schema.ScanHint{}, err
			}
			candidate, err := FromSQL(sqlVal)
			if err != nil {
				return schema.ScanHint{}, err
			}

			if cmp, err := Compare(candidate, currMin); err != nil {
				return schema.ScanHint{}, err
			} else if cmp < 0 {
				minVal = &sqlVal
			}

			if cmp, err := Compare(candidate, currMax); err != nil {
				return schema.ScanHint{}, err
			} else if cmp > 0 {
				maxVal = &sqlVal
			}
		}

		rng := schema.Range{
			Min: minVal,
			Max: maxVal,
		}

		hint.Ranges[offset] = rng
		return hint, nil

	default:
		return schema.ScanHint{}, nil
	}
}

func (p *ScanPlan) Walk(f func(Plan) (bool, error)) (bool, error) {
	return f(p)
}

func (p *ScanPlan) operands(left, right Expr, index schema.Index) (*ColumnExpr, Expr) {
	if l, ok := p.colName(left); ok && p.isIndexable(index, l) && p.isFoldable(right) {
		return l, right
	}
	if r, ok := p.colName(right); ok && p.isIndexable(index, r) && p.isFoldable(left) {
		return r, left
	}
	return nil, nil
}

func (p *ScanPlan) isIndexable(index schema.Index, expr *ColumnExpr) bool {
	for _, col := range index.Columns {
		if col.Equal(expr.Value) {
			return true
		}
	}
	return false
}

func (p *ScanPlan) isFoldable(expr Expr) bool {
	foldable := true
	_, _ = expr.Walk(func(e Expr) (bool, error) {
		switch e.(type) {
		case *ColumnExpr, *CallExpr, *SubqueryExpr:
			foldable = false
			return false, nil
		}
		return true, nil
	})
	return foldable
}

func (p *ScanPlan) colName(expr Expr) (*ColumnExpr, bool) {
	switch e := expr.(type) {
	case *IndexExpr:
		if left, ok := e.Left.(*ColumnExpr); ok {
			return left, true
		}
	default:
	}
	return nil, false
}
