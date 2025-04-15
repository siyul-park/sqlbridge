package plan

import (
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"math/big"
	"strconv"

	"github.com/xwb1989/sqlparser/dependency/querypb"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"

	"github.com/siyul-park/sqlbridge/eval"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser"
)

type Planner struct {
	catalog    schema.Catalog
	dispatcher *eval.Dispatcher
}

func NewPlanner(catalog schema.Catalog, dispatcher *eval.Dispatcher) *Planner {
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
		return &NOP{}, nil
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
		left = &Join{
			Left:  left,
			Right: right,
			Join:  sqlparser.JoinStr,
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

	return &Alias{Input: plan, As: as}, nil
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

	plan := &Join{
		Left:  left,
		Right: right,
		Join:  node.Join,
	}

	if node.Condition.On != nil {
		expr, err := p.planExpr(node.Condition.On)
		if err != nil {
			return nil, err
		}
		plan.On = expr
	}
	for _, using := range node.Condition.Using {
		plan.Using = append(plan.Using, &eval.Column{Value: &sqlparser.ColName{Name: using}})
	}

	return plan, nil
}

func (p *Planner) planTableName(node sqlparser.TableName) (Plan, error) {
	return &Scan{Catalog: p.catalog, Table: node}, nil
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
		return &Filter{
			Input: input,
			Expr:  expr,
		}, nil
	}
	return input, nil
}

func (p *Planner) planGroupBy(input Plan, node sqlparser.GroupBy) (Plan, error) {
	if len(node) > 0 {
		exprs := make([]eval.Expr, 0, len(node))
		for _, expr := range node {
			e, err := p.planExpr(expr)
			if err != nil {
				return nil, err
			}
			exprs = append(exprs, e)
		}
		input = &Group{
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
		return &Filter{
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
				items = append(items, &AliasItem{Expr: expr, As: e.As})
			default:
				return nil, driver.ErrSkip
			}
		}
		return &Projection{
			Input: input,
			Items: items,
		}, nil
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
		left = &Order{
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
		input = &Limit{
			Input:  input,
			Offset: offset,
			Count:  count,
		}
	}
	return input, nil
}

func (p *Planner) planExpr(expr sqlparser.Expr) (eval.Expr, error) {
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

func (p *Planner) planAndExpr(expr *sqlparser.AndExpr) (eval.Expr, error) {
	left, err := p.planExpr(expr.Left)
	if err != nil {
		return nil, err
	}
	right, err := p.planExpr(expr.Right)
	if err != nil {
		return nil, err
	}
	return &eval.And{Left: left, Right: right}, nil
}

func (p *Planner) planOrExpr(expr *sqlparser.OrExpr) (eval.Expr, error) {
	left, err := p.planExpr(expr.Left)
	if err != nil {
		return nil, err
	}
	right, err := p.planExpr(expr.Right)
	if err != nil {
		return nil, err
	}
	return &eval.Or{Left: left, Right: right}, nil
}

func (p *Planner) planNotExpr(expr *sqlparser.NotExpr) (eval.Expr, error) {
	input, err := p.planExpr(expr.Expr)
	if err != nil {
		return nil, err
	}
	return &eval.Not{Input: input}, nil
}

func (p *Planner) planParenExpr(expr *sqlparser.ParenExpr) (eval.Expr, error) {
	return p.planExpr(expr.Expr)
}

func (p *Planner) planComparisonExpr(expr *sqlparser.ComparisonExpr) (eval.Expr, error) {
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
		return &eval.Equal{Left: left, Right: right}, nil
	case sqlparser.NotEqualStr:
		return &eval.Not{Input: &eval.Equal{Left: left, Right: right}}, nil
	case sqlparser.LessThanStr:
		return &eval.LessThan{Left: left, Right: right}, nil
	case sqlparser.LessEqualStr:
		return &eval.LessThanEqual{Left: left, Right: right}, nil
	case sqlparser.GreaterThanStr:
		return &eval.GreaterThan{Left: left, Right: right}, nil
	case sqlparser.GreaterEqualStr:
		return &eval.GreaterThanEqual{Left: left, Right: right}, nil
	case sqlparser.InStr:
		return &eval.In{Left: left, Right: right}, nil
	case sqlparser.NotInStr:
		return &eval.Not{Input: &eval.In{Left: left, Right: right}}, nil
	case sqlparser.LikeStr:
		return &eval.Like{Left: left, Right: right}, nil
	case sqlparser.NotLikeStr:
		return &eval.Not{Input: &eval.Like{Left: left, Right: right}}, nil
	case sqlparser.RegexpStr:
		return &eval.Regexp{Left: left, Right: right}, nil
	case sqlparser.NotRegexpStr:
		return &eval.Not{Input: &eval.Regexp{Left: left, Right: right}}, nil
	case sqlparser.JSONExtractOp:
		return &eval.JSONExtract{Left: left, Right: right}, nil
	case sqlparser.JSONUnquoteExtractOp:
		return &eval.Convert{Input: &eval.JSONExtract{Left: left, Right: right}, Type: &sqlparser.ConvertType{Type: querypb.Type_name[int32(querypb.Type_VARCHAR)]}}, nil
	default:
		return nil, driver.ErrSkip
	}
}

func (p *Planner) planRangeCond(expr *sqlparser.RangeCond) (eval.Expr, error) {
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

	input := eval.Expr(&eval.And{
		Left: &eval.GreaterThanEqual{
			Left:  left,
			Right: from,
		},
		Right: &eval.LessThanEqual{
			Left:  left,
			Right: to,
		},
	})
	if expr.Operator == sqlparser.NotBetweenStr {
		input = &eval.Not{Input: input}
	}
	return input, nil
}

func (p *Planner) planIsExpr(expr *sqlparser.IsExpr) (eval.Expr, error) {
	input, err := p.planExpr(expr.Expr)
	if err != nil {
		return nil, err
	}

	switch expr.Operator {
	case sqlparser.IsNullStr:
		return &eval.IsNull{Input: input}, nil
	case sqlparser.IsNotNullStr:
		return &eval.Not{Input: &eval.IsNull{Input: input}}, nil
	case sqlparser.IsTrueStr:
		return &eval.IsTrue{Input: input}, nil
	case sqlparser.IsFalseStr:
		return &eval.Not{Input: &eval.IsTrue{Input: input}}, nil
	default:
		return nil, driver.ErrSkip
	}
}

func (p *Planner) planExistsExpr(expr *sqlparser.ExistsExpr) (eval.Expr, error) {
	input, err := p.planExpr(expr.Subquery)
	if err != nil {
		return nil, err
	}
	return &eval.Exists{Input: input}, nil
}

func (p *Planner) planSQLVal(expr *sqlparser.SQLVal) (eval.Expr, error) {
	switch expr.Type {
	case sqlparser.StrVal:
		val, err := sqltypes.NewValue(sqltypes.VarChar, expr.Val)
		if err != nil {
			return nil, err
		}
		return &eval.Literal{Value: val}, nil
	case sqlparser.IntVal:
		val, err := sqltypes.NewValue(sqltypes.Int64, expr.Val)
		if err != nil {
			return nil, err
		}
		return &eval.Literal{Value: val}, nil
	case sqlparser.FloatVal:
		val, err := sqltypes.NewValue(sqltypes.Float64, expr.Val)
		if err != nil {
			return nil, err
		}
		return &eval.Literal{Value: val}, nil
	case sqlparser.HexNum:
		if v, err := strconv.ParseUint(string(expr.Val), 16, 64); err != nil {
			return nil, err
		} else {
			return &eval.Literal{Value: sqltypes.NewUint64(v)}, nil
		}
	case sqlparser.HexVal:
		if data, err := hex.DecodeString(string(expr.Val)); err != nil {
			return nil, err
		} else if val, err := sqltypes.NewValue(sqltypes.VarBinary, data); err != nil {
			return nil, err
		} else {
			return &eval.Literal{Value: val}, nil
		}
	case sqlparser.ValArg:
		return &eval.Resolve{Value: string(expr.Val)}, nil
	case sqlparser.BitVal:
		if data, ok := new(big.Int).SetString(string(expr.Val), 2); !ok {
			return nil, fmt.Errorf("invalid bit string '%s'", expr.Val)
		} else if val, err := sqltypes.NewValue(sqltypes.Bit, data.Bytes()); err != nil {
			return nil, err
		} else {
			return &eval.Literal{Value: val}, nil
		}
	}
	return nil, driver.ErrSkip
}

func (p *Planner) planNullValue(_ *sqlparser.NullVal) (eval.Expr, error) {
	return &eval.Literal{Value: sqltypes.NULL}, nil
}

func (p *Planner) planBoolVal(expr sqlparser.BoolVal) (eval.Expr, error) {
	val := sqltypes.NewInt64(1)
	if expr {
		val = sqltypes.NewInt64(0)
	}
	return &eval.Literal{Value: val}, nil
}

func (p *Planner) planColName(expr *sqlparser.ColName) (eval.Expr, error) {
	return &eval.Column{Value: expr}, nil
}

func (p *Planner) planValTuple(expr sqlparser.ValTuple) (eval.Expr, error) {
	var exprs []eval.Expr
	for _, val := range expr {
		elem, err := p.planExpr(val)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, elem)
	}
	return &eval.Paren{Exprs: exprs}, nil
}

func (p *Planner) planListArg(expr sqlparser.ListArg) (eval.Expr, error) {
	return &eval.Resolve{Value: string(expr)}, nil
}

func (p *Planner) planBinaryExpr(expr *sqlparser.BinaryExpr) (eval.Expr, error) {
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
		return &eval.Plus{Left: left, Right: right}, nil
	case sqlparser.MinusStr:
		return &eval.Minus{Left: left, Right: right}, nil
	case sqlparser.MultStr:
		return &eval.Multiply{Left: left, Right: right}, nil
	case sqlparser.DivStr:
		return &eval.Divide{Left: left, Right: right}, nil
	case sqlparser.IntDivStr:
		return &eval.Divide{Left: left, Right: right}, nil
	case sqlparser.ModStr:
		return &eval.Modulo{Left: left, Right: right}, nil
	case sqlparser.ShiftLeftStr:
		return &eval.ShiftLeft{Left: left, Right: right}, nil
	case sqlparser.ShiftRightStr:
		return &eval.ShiftRight{Left: left, Right: right}, nil
	case sqlparser.BitAndStr:
		return &eval.BitAnd{Left: left, Right: right}, nil
	case sqlparser.BitOrStr:
		return &eval.BitOr{Left: left, Right: right}, nil
	case sqlparser.BitXorStr:
		return &eval.BitXor{Left: left, Right: right}, nil
	default:
		return nil, driver.ErrSkip
	}
}

func (p *Planner) planUnaryExpr(expr *sqlparser.UnaryExpr) (eval.Expr, error) {
	input, err := p.planExpr(expr.Expr)
	if err != nil {
		return nil, err
	}

	switch expr.Operator {
	case sqlparser.UPlusStr:
		return input, nil
	case sqlparser.UMinusStr:
		return &eval.Multiply{Left: input, Right: &eval.Literal{Value: sqltypes.NewInt64(-1)}}, nil
	case sqlparser.TildaStr:
		return &eval.BitNot{Input: input}, nil
	case sqlparser.BangStr:
		return &eval.Not{Input: input}, nil
	case sqlparser.BinaryStr, sqlparser.UBinaryStr:
		return &eval.Convert{Input: input, Type: &sqlparser.ConvertType{Type: querypb.Type_name[int32(querypb.Type_VARBINARY)]}}, nil
	default:
		return nil, driver.ErrSkip
	}
}

func (p *Planner) planIntervalExpr(expr *sqlparser.IntervalExpr) (eval.Expr, error) {
	input, err := p.planExpr(expr.Expr)
	if err != nil {
		return nil, err
	}
	return &eval.Interval{Input: input, Unit: expr.Unit}, nil
}

func (p *Planner) planFuncExpr(expr *sqlparser.FuncExpr) (eval.Expr, error) {
	exprs := make([]eval.Expr, 0, len(expr.Exprs))
	for _, expr := range expr.Exprs {
		switch e := expr.(type) {
		case *sqlparser.StarExpr:
			exprs = append(exprs, &eval.Columns{Value: e.TableName})
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
	input := eval.Expr(&eval.Paren{Exprs: exprs})
	if expr.Distinct {
		input = &eval.Distinct{Input: input}
	}
	return &eval.Func{
		Dispatcher: p.dispatcher,
		Qualifier:  expr.Qualifier,
		Name:       expr.Name,
		Aggregate:  expr.IsAggregate(),
		Input:      input,
	}, nil
}

func (p *Planner) planCaseExpr(expr *sqlparser.CaseExpr) (eval.Expr, error) {
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
		right = &eval.If{
			When: &eval.Equal{Left: when, Right: cond},
			Then: then,
			Else: right,
		}
	}
	return right, nil
}

func (p *Planner) planValuesFuncExpr(expr *sqlparser.ValuesFuncExpr) (eval.Expr, error) {
	return &eval.Column{Value: expr.Name}, nil
}

func (p *Planner) planConvertExpr(expr *sqlparser.ConvertExpr) (eval.Expr, error) {
	input, err := p.planExpr(expr.Expr)
	if err != nil {
		return nil, err
	}
	return &eval.Convert{Input: input, Type: expr.Type}, nil
}

func (p *Planner) planSubstrExpr(expr *sqlparser.SubstrExpr) (eval.Expr, error) {
	exprs := []eval.Expr{&eval.Column{Value: expr.Name}}
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
	return &eval.Func{
		Dispatcher: p.dispatcher,
		Name:       sqlparser.NewColIdent("substr"),
		Input:      &eval.Paren{Exprs: exprs},
	}, nil
}

func (p *Planner) planMatchExpr(expr *sqlparser.MatchExpr) (eval.Expr, error) {
	left := make([]eval.Expr, 0, len(expr.Columns))
	for _, expr := range expr.Columns {
		switch e := expr.(type) {
		case *sqlparser.StarExpr:
			left = append(left, &eval.Columns{Value: e.TableName})
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
	return &eval.Match{
		Left:  left,
		Right: right,
	}, nil
}

func (p *Planner) planGroupConcatExpr(expr *sqlparser.GroupConcatExpr) (eval.Expr, error) {
	exprs := make([]eval.Expr, 0, len(expr.Exprs))
	for _, expr := range expr.Exprs {
		switch e := expr.(type) {
		case *sqlparser.StarExpr:
			exprs = append(exprs, &eval.Columns{Value: e.TableName})
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

	input := eval.Expr(&eval.Paren{Exprs: exprs})

	for _, order := range expr.OrderBy {
		right, err := p.planExpr(order.Expr)
		if err != nil {
			return nil, err
		}
		input = &eval.Order{
			Left:      input,
			Right:     right,
			Direction: order.Direction,
		}
	}

	if expr.Distinct == sqlparser.DistinctStr {
		input = &eval.Distinct{Input: input}
	}

	return &eval.Concat{
		Input:     input,
		Separator: expr.Separator,
	}, nil
}

func (p *Planner) planDefault(_ *sqlparser.Default) (eval.Expr, error) {
	return &eval.Literal{Value: sqltypes.NULL}, nil
}
