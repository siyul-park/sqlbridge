package engine

import (
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"math/big"
	"strconv"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/querypb"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
)

type Planner struct {
	catalog    schema.Catalog
	dispatcher *Dispatcher
}

func NewPlanner(catalog schema.Catalog, dispatcher *Dispatcher) *Planner {
	return &Planner{
		catalog:    catalog,
		dispatcher: dispatcher,
	}
}

func (p *Planner) Plan(node sqlparser.Statement) (Plan, error) {
	switch n := node.(type) {
	case sqlparser.SelectStatement:
		return p.planSelectStatement(n)
	case *sqlparser.Insert:
	case *sqlparser.Update:
	case *sqlparser.Delete:
	case *sqlparser.Set:
	case *sqlparser.DBDDL:
	case *sqlparser.DDL:
	case *sqlparser.Show:
	case *sqlparser.Use:
	case *sqlparser.Begin:
	case *sqlparser.Commit:
	case *sqlparser.Rollback:
	case *sqlparser.OtherRead:
	case *sqlparser.OtherAdmin:
	case *sqlparser.Stream:
	}
	return nil, driver.ErrSkip
}

func (p *Planner) planSelectStatement(node sqlparser.SelectStatement) (Plan, error) {
	switch n := node.(type) {
	case *sqlparser.Union:
	case *sqlparser.Select:
		return p.planSelect(n)
	case *sqlparser.ParenSelect:
	}
	return nil, driver.ErrSkip
}

func (p *Planner) planSelect(node *sqlparser.Select) (Plan, error) {
	if input, err := p.planTableExprs(node.From); err != nil {
		return nil, err
	} else if input, err = p.planWhere(input, node.Where); err != nil {
		return nil, err
	} else if input, err = p.planGroupBy(input, node.GroupBy); err != nil {
		return nil, err
	} else if input, err = p.planHaving(input, node.Having); err != nil {
		return nil, err
	} else if input, err = p.planSelectExprs(input, node.SelectExprs); err != nil {
		return nil, err
	} else if input, err = p.planDistinct(input, node.Distinct); err != nil {
		return nil, err
	} else if input, err = p.planOrderBy(input, node.OrderBy); err != nil {
		return nil, err
	} else if input, err = p.planLimit(input, node.Limit); err != nil {
		return nil, err
	} else {
		return input, nil
	}
}

func (p *Planner) planTableExprs(node sqlparser.TableExprs) (Plan, error) {
	if len(node) == 0 {
		return &NOPPlan{}, nil
	}

	left, err := p.planTableExpr(node[0])
	if err != nil {
		return nil, err
	}

	for _, expr := range node[1:] {
		right, err := p.planTableExpr(expr)
		if err != nil {
			return nil, err
		}
		left = &JoinPlan{
			Left:  left,
			Right: right,
		}
	}
	return left, nil
}

func (p *Planner) planTableExpr(node sqlparser.TableExpr) (Plan, error) {
	switch n := node.(type) {
	case *sqlparser.AliasedTableExpr:
		return p.planAliasedTableExpr(n)
	case *sqlparser.ParenTableExpr:
		return p.planParenTableExpr(n)
	case *sqlparser.JoinTableExpr:
		return p.planJoinTableExpr(n)
	}
	return nil, driver.ErrSkip
}

func (p *Planner) planSimpleTableExpr(node sqlparser.SimpleTableExpr) (Plan, error) {
	switch n := node.(type) {
	case sqlparser.TableName:
		return p.planTableName(n)
	case *sqlparser.Subquery:
		return p.planSubquery(n)
	}
	return nil, driver.ErrSkip
}

func (p *Planner) planAliasedTableExpr(node *sqlparser.AliasedTableExpr) (Plan, error) {
	plan, err := p.planSimpleTableExpr(node.Expr)
	if err != nil {
		return nil, err
	}

	as := node.As
	if as.IsEmpty() {
		switch expr := node.Expr.(type) {
		case sqlparser.TableName:
			as = expr.Name
		default:
			as = sqlparser.NewTableIdent(sqlparser.String(expr))
		}
	}

	return &AliasPlan{Input: plan, As: as}, nil
}

func (p *Planner) planParenTableExpr(node *sqlparser.ParenTableExpr) (Plan, error) {
	return p.planTableExprs(node.Exprs)
}

func (p *Planner) planJoinTableExpr(node *sqlparser.JoinTableExpr) (Plan, error) {
	left, err := p.planTableExpr(node.LeftExpr)
	if err != nil {
		return nil, err
	}
	right, err := p.planTableExpr(node.RightExpr)
	if err != nil {
		return nil, err
	}

	var plan Plan = &JoinPlan{
		Left:  left,
		Right: right,
	}

	if node.Condition.On != nil {
		if expr, err := p.planExpr(node.Condition.On); err != nil {
			return nil, err
		} else {
			plan = &FilterPlan{
				Input: plan,
				Expr:  expr,
			}
		}
	}

	for _, u := range node.Condition.Using {
		expr := &IdenticalExpr{Input: &ColumnExpr{Value: &sqlparser.ColName{Name: u}}}
		plan = &FilterPlan{
			Input: plan,
			Expr:  expr,
		}
	}
	return plan, nil
}

func (p *Planner) planTableName(node sqlparser.TableName) (Plan, error) {
	return &ScanPlan{Catalog: p.catalog, Table: node}, nil
}

func (p *Planner) planSubquery(node *sqlparser.Subquery) (Plan, error) {
	return p.planSelectStatement(node.Select)
}

func (p *Planner) planWhere(input Plan, node *sqlparser.Where) (Plan, error) {
	if node != nil {
		expr, err := p.planExpr(node.Expr)
		if err != nil {
			return nil, err
		}

		exprs := p.splitByTables(expr)
		_, _ = input.Walk(func(plan Plan) (bool, error) {
			switch p := plan.(type) {
			case *AliasPlan:
				if i, ok := p.Input.(*ScanPlan); ok {
					for table, expr := range exprs {
						if (table.Name.IsEmpty() || table.Name == p.As) && (table.Qualifier.IsEmpty() || table.Qualifier == i.Table.Qualifier) {
							expr = expr.Copy()
							_, _ = expr.Walk(func(expr Expr) (bool, error) {
								if e, ok := expr.(*ColumnExpr); ok {
									e.Value.Qualifier = sqlparser.TableName{}
								}
								return true, nil
							})
							if i.Expr == nil {
								i.Expr = expr
							} else {
								i.Expr = &AndExpr{Left: i.Expr, Right: expr}
							}
						}
					}
				}
			case *JoinPlan:
				return true, nil
			}
			return false, nil
		})

		return &FilterPlan{
			Input: input,
			Expr:  expr,
		}, nil
	}
	return input, nil
}

func (p *Planner) planGroupBy(input Plan, node sqlparser.GroupBy) (Plan, error) {
	if len(node) > 0 {
		exprs := make([]Expr, 0, len(node))
		for _, expr := range node {
			e, err := p.planExpr(expr)
			if err != nil {
				return nil, err
			}
			exprs = append(exprs, e)
		}
		input = &GroupPlan{
			Input: input,
			Exprs: exprs,
		}
	}
	return input, nil
}

func (p *Planner) planHaving(input Plan, node *sqlparser.Where) (Plan, error) {
	if node != nil {
		expr, err := p.planExpr(node.Expr)
		if err != nil {
			return nil, err
		}

		return &FilterPlan{
			Input: input,
			Expr:  expr,
		}, nil
	}
	return input, nil
}

func (p *Planner) planSelectExprs(input Plan, node sqlparser.SelectExprs) (Plan, error) {
	if len(node) > 0 {
		items := make([]ProjectionItem, 0, len(node))
		for _, expr := range node {
			switch e := expr.(type) {
			case *sqlparser.StarExpr:
				items = append(items, &StartItem{Table: e.TableName})
			case *sqlparser.AliasedExpr:
				expr, err := p.planExpr(e.Expr)
				if err != nil {
					return nil, err
				}
				as := e.As
				if as.IsEmpty() {
					as = sqlparser.NewColIdent(sqlparser.String(e.Expr))
					switch expr := expr.(type) {
					case *IndexExpr:
						if left, ok := expr.Left.(*ColumnExpr); ok {
							as = left.Value.Name
						}
					case *ColumnExpr:
						as = expr.Value.Name
					default:
					}
				}
				items = append(items, &AliasItem{Expr: expr, As: as})
			default:
				return nil, driver.ErrSkip
			}
		}
		return &ProjectionPlan{
			Input: input,
			Items: items,
		}, nil
	}
	return input, nil
}

func (p *Planner) planDistinct(input Plan, distinct string) (Plan, error) {
	if distinct == sqlparser.DistinctStr {
		input = &DistinctPlan{Input: input}
	}
	return input, nil
}

func (p *Planner) planOrderBy(input Plan, node sqlparser.OrderBy) (Plan, error) {
	left := input
	for i := len(node) - 1; i >= 0; i-- {
		expr, err := p.planExpr(node[i].Expr)
		if err != nil {
			return nil, err
		}
		left = &OrderPlan{
			Input:     left,
			Expr:      expr,
			Direction: node[i].Direction,
		}
	}
	return left, nil
}

func (p *Planner) planLimit(input Plan, node *sqlparser.Limit) (Plan, error) {
	if node != nil {
		offset, err := p.planExpr(node.Offset)
		if err != nil {
			return nil, err
		}
		count, err := p.planExpr(node.Rowcount)
		if err != nil {
			return nil, err
		}
		input = &LimitPlan{
			Input:  input,
			Offset: offset,
			Count:  count,
		}
	}
	return input, nil
}

func (p *Planner) planExpr(expr sqlparser.Expr) (Expr, error) {
	if expr == nil {
		return nil, nil
	}
	switch expr := expr.(type) {
	case *sqlparser.AndExpr:
		return p.planAndExpr(expr)
	case *sqlparser.OrExpr:
		return p.planOrExpr(expr)
	case *sqlparser.NotExpr:
		return p.planNotExpr(expr)
	case *sqlparser.ParenExpr:
		return p.planParenExpr(expr)
	case *sqlparser.ComparisonExpr:
		return p.planComparisonExpr(expr)
	case *sqlparser.RangeCond:
		return p.planRangeCond(expr)
	case *sqlparser.IsExpr:
		return p.planIsExpr(expr)
	case *sqlparser.ExistsExpr:
		return p.planExistsExpr(expr)
	case *sqlparser.SQLVal:
		return p.planSQLVal(expr)
	case *sqlparser.NullVal:
		return p.planNullValue(expr)
	case sqlparser.BoolVal:
		return p.planBoolVal(expr)
	case *sqlparser.ColName:
		return p.planColName(expr)
	case sqlparser.ValTuple:
		return p.planValTuple(expr)
	case *sqlparser.Subquery:
		return p.planSubqueryExpr(expr)
	case sqlparser.ListArg:
		return p.planListArg(expr)
	case *sqlparser.BinaryExpr:
		return p.planBinaryExpr(expr)
	case *sqlparser.UnaryExpr:
		return p.planUnaryExpr(expr)
	case *sqlparser.IntervalExpr:
		return p.planIntervalExpr(expr)
	case *sqlparser.CollateExpr:
	case *sqlparser.FuncExpr:
		return p.planFuncExpr(expr)
	case *sqlparser.CaseExpr:
		return p.planCaseExpr(expr)
	case *sqlparser.ValuesFuncExpr:
		return p.planValuesFuncExpr(expr)
	case *sqlparser.ConvertExpr:
		return p.planConvertExpr(expr)
	case *sqlparser.SubstrExpr:
		return p.planSubstrExpr(expr)
	case *sqlparser.ConvertUsingExpr:
	case *sqlparser.MatchExpr:
		return p.planMatchExpr(expr)
	case *sqlparser.GroupConcatExpr:
		return p.planGroupConcatExpr(expr)
	case *sqlparser.Default:
		return p.planDefault(expr)
	}
	return nil, driver.ErrSkip
}

func (p *Planner) planAndExpr(expr *sqlparser.AndExpr) (Expr, error) {
	left, err := p.planExpr(expr.Left)
	if err != nil {
		return nil, err
	}
	right, err := p.planExpr(expr.Right)
	if err != nil {
		return nil, err
	}
	return &AndExpr{Left: left, Right: right}, nil
}

func (p *Planner) planOrExpr(expr *sqlparser.OrExpr) (Expr, error) {
	left, err := p.planExpr(expr.Left)
	if err != nil {
		return nil, err
	}
	right, err := p.planExpr(expr.Right)
	if err != nil {
		return nil, err
	}
	return &OrExpr{Left: left, Right: right}, nil
}

func (p *Planner) planNotExpr(expr *sqlparser.NotExpr) (Expr, error) {
	input, err := p.planExpr(expr.Expr)
	if err != nil {
		return nil, err
	}
	return &NotExpr{Input: input}, nil
}

func (p *Planner) planParenExpr(expr *sqlparser.ParenExpr) (Expr, error) {
	return p.planExpr(expr.Expr)
}

func (p *Planner) planComparisonExpr(expr *sqlparser.ComparisonExpr) (Expr, error) {
	left, err := p.planExpr(expr.Left)
	if err != nil {
		return nil, err
	}
	right, err := p.planExpr(expr.Right)
	if err != nil {
		return nil, err
	}

	switch expr.Operator {
	case sqlparser.EqualStr, sqlparser.NullSafeEqualStr:
		return &EqualExpr{Left: left, Right: right}, nil
	case sqlparser.NotEqualStr:
		return &NotExpr{Input: &EqualExpr{Left: left, Right: right}}, nil
	case sqlparser.LessThanStr:
		return &LessThanExpr{Left: left, Right: right}, nil
	case sqlparser.LessEqualStr:
		return &LessThanOrEqualExpr{Left: left, Right: right}, nil
	case sqlparser.GreaterThanStr:
		return &GreaterThanExpr{Left: left, Right: right}, nil
	case sqlparser.GreaterEqualStr:
		return &GreaterThanOrEqualExpr{Left: left, Right: right}, nil
	case sqlparser.InStr:
		return &InExpr{Left: left, Right: right}, nil
	case sqlparser.NotInStr:
		return &NotExpr{Input: &InExpr{Left: left, Right: right}}, nil
	case sqlparser.LikeStr:
		return &LikeExpr{Left: left, Right: right}, nil
	case sqlparser.NotLikeStr:
		return &NotExpr{Input: &LikeExpr{Left: left, Right: right}}, nil
	case sqlparser.RegexpStr:
		return &RegexpExpr{Left: left, Right: right}, nil
	case sqlparser.NotRegexpStr:
		return &NotExpr{Input: &RegexpExpr{Left: left, Right: right}}, nil
	case sqlparser.JSONExtractOp:
		return &JSONExtractExpr{Left: left, Right: right}, nil
	case sqlparser.JSONUnquoteExtractOp:
		return &ConvertExpr{Input: &JSONExtractExpr{Left: left, Right: right}, Type: &sqlparser.ConvertType{Type: querypb.Type_name[int32(querypb.Type_VARCHAR)]}}, nil
	default:
		return nil, driver.ErrSkip
	}
}

func (p *Planner) planRangeCond(expr *sqlparser.RangeCond) (Expr, error) {
	left, err := p.planExpr(expr.Left)
	if err != nil {
		return nil, err
	}
	from, err := p.planExpr(expr.From)
	if err != nil {
		return nil, err
	}
	to, err := p.planExpr(expr.To)
	if err != nil {
		return nil, err
	}

	input := Expr(&AndExpr{
		Left: &GreaterThanOrEqualExpr{
			Left:  left,
			Right: from,
		},
		Right: &LessThanOrEqualExpr{
			Left:  left,
			Right: to,
		},
	})

	if expr.Operator == sqlparser.NotBetweenStr {
		input = &NotExpr{Input: input}
	}
	return input, nil
}

func (p *Planner) planIsExpr(expr *sqlparser.IsExpr) (Expr, error) {
	input, err := p.planExpr(expr.Expr)
	if err != nil {
		return nil, err
	}

	switch expr.Operator {
	case sqlparser.IsNullStr:
		return &CallExpr{
			Dispatcher: p.dispatcher,
			Name:       NVL2,
			Input:      &TupleExpr{Exprs: []Expr{input, &LiteralExpr{Value: sqltypes.NewInt64(1)}, &LiteralExpr{Value: sqltypes.NewInt64(0)}}},
		}, nil
	case sqlparser.IsNotNullStr:
		return &CallExpr{
			Dispatcher: p.dispatcher,
			Name:       NVL2,
			Input:      &TupleExpr{Exprs: []Expr{input, &LiteralExpr{Value: sqltypes.NewInt64(0)}, &LiteralExpr{Value: sqltypes.NewInt64(1)}}},
		}, nil
	case sqlparser.IsTrueStr:
		return &IfExpr{
			When: input,
			Then: &LiteralExpr{Value: sqltypes.NewInt64(1)},
			Else: &LiteralExpr{Value: sqltypes.NewInt64(0)},
		}, nil
	case sqlparser.IsFalseStr:
		return &IfExpr{
			When: input,
			Then: &LiteralExpr{Value: sqltypes.NewInt64(0)},
			Else: &LiteralExpr{Value: sqltypes.NewInt64(1)},
		}, nil
	default:
		return nil, driver.ErrSkip
	}
}

func (p *Planner) planExistsExpr(expr *sqlparser.ExistsExpr) (Expr, error) {
	input, err := p.planExpr(expr.Subquery)
	if err != nil {
		return nil, err
	}
	return &IfExpr{
		When: input,
		Then: &LiteralExpr{Value: sqltypes.NewInt64(1)},
		Else: &LiteralExpr{Value: sqltypes.NewInt64(0)},
	}, nil
}

func (p *Planner) planSQLVal(expr *sqlparser.SQLVal) (Expr, error) {
	switch expr.Type {
	case sqlparser.StrVal:
		val, err := sqltypes.NewValue(sqltypes.VarChar, expr.Val)
		if err != nil {
			return nil, err
		}
		return &LiteralExpr{Value: val}, nil
	case sqlparser.IntVal:
		val, err := sqltypes.NewValue(sqltypes.Int64, expr.Val)
		if err != nil {
			return nil, err
		}
		return &LiteralExpr{Value: val}, nil
	case sqlparser.FloatVal:
		val, err := sqltypes.NewValue(sqltypes.Float64, expr.Val)
		if err != nil {
			return nil, err
		}
		return &LiteralExpr{Value: val}, nil
	case sqlparser.HexNum:
		if v, err := strconv.ParseUint(string(expr.Val), 16, 64); err != nil {
			return nil, err
		} else {
			return &LiteralExpr{Value: sqltypes.NewUint64(v)}, nil
		}
	case sqlparser.HexVal:
		if data, err := hex.DecodeString(string(expr.Val)); err != nil {
			return nil, err
		} else if val, err := sqltypes.NewValue(sqltypes.VarBinary, data); err != nil {
			return nil, err
		} else {
			return &LiteralExpr{Value: val}, nil
		}
	case sqlparser.ValArg:
		return &ValArgExpr{Value: string(expr.Val)}, nil
	case sqlparser.BitVal:
		if data, ok := new(big.Int).SetString(string(expr.Val), 2); !ok {
			return nil, fmt.Errorf("invalid bit string '%s'", expr.Val)
		} else if val, err := sqltypes.NewValue(sqltypes.Bit, data.Bytes()); err != nil {
			return nil, err
		} else {
			return &LiteralExpr{Value: val}, nil
		}
	}
	return nil, driver.ErrSkip
}

func (p *Planner) planNullValue(_ *sqlparser.NullVal) (Expr, error) {
	return &LiteralExpr{Value: sqltypes.NULL}, nil
}

func (p *Planner) planBoolVal(expr sqlparser.BoolVal) (Expr, error) {
	val := sqltypes.NewInt64(1)
	if expr {
		val = sqltypes.NewInt64(0)
	}
	return &LiteralExpr{Value: val}, nil
}

func (p *Planner) planColName(expr *sqlparser.ColName) (Expr, error) {
	return &IndexExpr{Left: &ColumnExpr{Value: expr}, Right: &LiteralExpr{Value: sqltypes.NewInt64(0)}}, nil
}

func (p *Planner) planValTuple(expr sqlparser.ValTuple) (Expr, error) {
	var exprs []Expr
	for _, val := range expr {
		elem, err := p.planExpr(val)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, elem)
	}
	return &TupleExpr{Exprs: exprs}, nil
}

func (p *Planner) planSubqueryExpr(expr *sqlparser.Subquery) (Expr, error) {
	plan, err := p.planSubquery(expr)
	if err != nil {
		return nil, err
	}
	return &SubqueryExpr{Input: plan}, nil
}

func (p *Planner) planListArg(expr sqlparser.ListArg) (Expr, error) {
	return &ValArgExpr{Value: string(expr)}, nil
}

func (p *Planner) planBinaryExpr(expr *sqlparser.BinaryExpr) (Expr, error) {
	left, err := p.planExpr(expr.Left)
	if err != nil {
		return nil, err
	}
	right, err := p.planExpr(expr.Right)
	if err != nil {
		return nil, err
	}

	switch expr.Operator {
	case sqlparser.PlusStr:
		return &AddExpr{Left: left, Right: right}, nil
	case sqlparser.MinusStr:
		return &SubExpr{Left: left, Right: right}, nil
	case sqlparser.MultStr:
		return &MulExpr{Left: left, Right: right}, nil
	case sqlparser.DivStr:
		return &DivExpr{Left: left, Right: right}, nil
	case sqlparser.IntDivStr:
		return &DivExpr{Left: left, Right: right}, nil
	case sqlparser.ModStr:
		return &ModExpr{Left: left, Right: right}, nil
	case sqlparser.ShiftLeftStr:
		return &ShiftLeftExpr{Left: left, Right: right}, nil
	case sqlparser.ShiftRightStr:
		return &ShiftRightExpr{Left: left, Right: right}, nil
	case sqlparser.BitAndStr:
		return &CallExpr{
			Dispatcher: p.dispatcher,
			Name:       BitAnd,
			Input:      &TupleExpr{Exprs: []Expr{left, right}},
		}, nil
	case sqlparser.BitOrStr:
		return &CallExpr{
			Dispatcher: p.dispatcher,
			Name:       BitOr,
			Input:      &TupleExpr{Exprs: []Expr{left, right}},
		}, nil
	case sqlparser.BitXorStr:
		return &CallExpr{
			Dispatcher: p.dispatcher,
			Name:       BitXor,
			Input:      &TupleExpr{Exprs: []Expr{left, right}},
		}, nil
	default:
		return nil, driver.ErrSkip
	}
}

func (p *Planner) planUnaryExpr(expr *sqlparser.UnaryExpr) (Expr, error) {
	input, err := p.planExpr(expr.Expr)
	if err != nil {
		return nil, err
	}

	switch expr.Operator {
	case sqlparser.UPlusStr:
		return &MulExpr{Left: input, Right: &LiteralExpr{Value: sqltypes.NewInt64(1)}}, nil
	case sqlparser.UMinusStr:
		return &MulExpr{Left: input, Right: &LiteralExpr{Value: sqltypes.NewInt64(-1)}}, nil
	case sqlparser.TildaStr:
		return &BitNotExpr{Input: input}, nil
	case sqlparser.BangStr:
		return &NotExpr{Input: input}, nil
	case sqlparser.BinaryStr, sqlparser.UBinaryStr:
		return &ConvertExpr{Input: input, Type: &sqlparser.ConvertType{Type: querypb.Type_name[int32(querypb.Type_VARBINARY)]}}, nil
	default:
		return nil, driver.ErrSkip
	}
}

func (p *Planner) planIntervalExpr(expr *sqlparser.IntervalExpr) (Expr, error) {
	input, err := p.planExpr(expr.Expr)
	if err != nil {
		return nil, err
	}
	return &IntervalExpr{Input: input, Unit: expr.Unit}, nil
}

func (p *Planner) planFuncExpr(expr *sqlparser.FuncExpr) (Expr, error) {
	exprs := make([]Expr, 0, len(expr.Exprs))
	for _, expr := range expr.Exprs {
		switch e := expr.(type) {
		case *sqlparser.StarExpr:
			exprs = append(exprs, &TableExpr{Value: e.TableName})
		case *sqlparser.AliasedExpr:
			expr, err := p.planExpr(e.Expr)
			if err != nil {
				return nil, err
			}
			exprs = append(exprs, expr)
		default:
			return nil, driver.ErrSkip
		}
	}

	input := Expr(&SpreadExpr{Exprs: exprs})
	if expr.Distinct {
		input = &DistinctExpr{Input: input}
	}

	return &CallExpr{
		Dispatcher: p.dispatcher,
		Qualifier:  expr.Qualifier,
		Name:       expr.Name,
		Aggregate:  expr.IsAggregate(),
		Input:      input,
	}, nil
}

func (p *Planner) planCaseExpr(expr *sqlparser.CaseExpr) (Expr, error) {
	when, err := p.planExpr(expr.Expr)
	if err != nil {
		return nil, err
	}
	right, err := p.planExpr(expr.Else)
	if err != nil {
		return nil, err
	}

	for i := len(expr.Whens) - 1; i >= 0; i-- {
		cond, err := p.planExpr(expr.Whens[i].Cond)
		if err != nil {
			return nil, err
		}
		then, err := p.planExpr(expr.Whens[i].Val)
		if err != nil {
			return nil, err
		}
		right = &IfExpr{
			When: &EqualExpr{Left: when, Right: cond},
			Then: then,
			Else: right,
		}
	}
	return right, nil
}

func (p *Planner) planValuesFuncExpr(expr *sqlparser.ValuesFuncExpr) (Expr, error) {
	return p.planExpr(expr.Name)
}

func (p *Planner) planConvertExpr(expr *sqlparser.ConvertExpr) (Expr, error) {
	input, err := p.planExpr(expr.Expr)
	if err != nil {
		return nil, err
	}
	return &ConvertExpr{Input: input, Type: expr.Type}, nil
}

func (p *Planner) planSubstrExpr(expr *sqlparser.SubstrExpr) (Expr, error) {
	exprs := []Expr{&ColumnExpr{Value: expr.Name}}
	if expr.From != nil {
		from, err := p.planExpr(expr.From)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, from)
	}
	if expr.To != nil {
		to, err := p.planExpr(expr.To)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, to)
	}
	return &CallExpr{
		Dispatcher: p.dispatcher,
		Name:       Substr,
		Input:      &TupleExpr{Exprs: exprs},
	}, nil
}

func (p *Planner) planMatchExpr(expr *sqlparser.MatchExpr) (Expr, error) {
	left := make([]Expr, 0, len(expr.Columns))
	for _, expr := range expr.Columns {
		switch e := expr.(type) {
		case *sqlparser.StarExpr:
			left = append(left, &TableExpr{Value: e.TableName})
		case *sqlparser.AliasedExpr:
			expr, err := p.planExpr(e.Expr)
			if err != nil {
				return nil, err
			}
			left = append(left, expr)
		default:
			return nil, driver.ErrSkip
		}
	}

	right, err := p.planExpr(expr.Expr)
	if err != nil {
		return nil, err
	}

	return &MatchExpr{
		Left:  &SpreadExpr{Exprs: left},
		Right: right,
	}, nil
}

func (p *Planner) planGroupConcatExpr(expr *sqlparser.GroupConcatExpr) (Expr, error) {
	exprs := make([]Expr, 0, len(expr.Exprs))
	for _, expr := range expr.Exprs {
		switch e := expr.(type) {
		case *sqlparser.StarExpr:
			exprs = append(exprs, &TableExpr{Value: e.TableName})
		case *sqlparser.AliasedExpr:
			expr, err := p.planExpr(e.Expr)
			if err != nil {
				return nil, err
			}
			exprs = append(exprs, expr)
		default:
			return nil, driver.ErrSkip
		}
	}

	input := Expr(&SpreadExpr{Exprs: exprs})
	for _, order := range expr.OrderBy {
		right, err := p.planExpr(order.Expr)
		if err != nil {
			return nil, err
		}
		input = &OrderExpr{
			Left:      input,
			Right:     right,
			Direction: order.Direction,
		}
	}
	if expr.Distinct == sqlparser.DistinctStr {
		input = &DistinctExpr{Input: input}
	}

	return &CallExpr{
		Dispatcher: p.dispatcher,
		Name:       ConcatWs,
		Input:      &SpreadExpr{Exprs: []Expr{&LiteralExpr{Value: sqltypes.NewVarChar(expr.Separator)}, input}},
	}, nil
}

func (p *Planner) planDefault(_ *sqlparser.Default) (Expr, error) {
	return &LiteralExpr{Value: sqltypes.NULL}, nil
}

func (p *Planner) splitByTables(expr Expr) map[sqlparser.TableName]Expr {
	exprs := make(map[sqlparser.TableName]Expr)
	queue := []Expr{expr}
	for len(queue) > 0 {
		expr, queue = queue[0], queue[1:]

		if e, ok := expr.(*AndExpr); ok {
			queue = append(queue, e.Left, e.Right)
			continue
		}

		tables := make(map[sqlparser.TableName]struct{})
		_, _ = expr.Walk(func(expr Expr) (bool, error) {
			if e, ok := expr.(*ColumnExpr); ok {
				tables[e.Value.Qualifier] = struct{}{}
			}
			return true, nil
		})

		if len(tables) == 1 {
			for t := range tables {
				if existing, ok := exprs[t]; ok {
					exprs[t] = &AndExpr{Left: existing, Right: expr}
				} else {
					exprs[t] = expr
				}
			}
		}
	}
	return exprs
}
