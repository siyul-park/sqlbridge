package plan

import (
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"math/big"
	"strconv"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type Planner struct {
	catalog schema.Catalog
}

func NewPlanner(catalog schema.Catalog) *Planner {
	return &Planner{catalog: catalog}
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
		plan.Using = append(plan.Using, &Column{Value: &sqlparser.ColName{Name: using}})
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
	return input, nil
}

func (p *Planner) planLimit(input Plan, node *sqlparser.Limit) (Plan, error) {
	return input, nil
}

func (p *Planner) planExpr(expr sqlparser.Expr) (Expr, error) {
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
	case *sqlparser.IsExpr:
	case *sqlparser.ExistsExpr:
	case *sqlparser.SQLVal:
		return p.planSQLVal(expr)
	case *sqlparser.NullVal:
		return p.planNullValue(expr)
	case sqlparser.BoolVal:
		return p.planBoolVal(expr)
	case *sqlparser.ColName:
		return p.planColName(expr)
	case sqlparser.ValTuple:
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
	return &And{Left: left, Right: right}, nil
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
	return &Or{Left: left, Right: right}, nil
}

func (p *Planner) planNotExpr(expr *sqlparser.NotExpr) (Expr, error) {
	input, err := p.planExpr(expr.Expr)
	if err != nil {
		return nil, err
	}
	return &Not{Input: input}, nil
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
		return &Equal{Left: left, Right: right}, nil
	case sqlparser.NotEqualStr:
		return &Not{Input: &Equal{Left: left, Right: right}}, nil
	case sqlparser.LessThanStr:
		return &LessThan{Left: left, Right: right}, nil
	case sqlparser.LessEqualStr:
		return &Or{Left: &LessThan{Left: left, Right: right}, Right: &Equal{Left: left, Right: right}}, nil
	case sqlparser.GreaterThanStr:
		return &Not{Input: &Or{Left: &LessThan{Left: left, Right: right}, Right: &Equal{Left: left, Right: right}}}, nil
	case sqlparser.GreaterEqualStr:
		return &Not{Input: &LessThan{Left: left, Right: right}}, nil
	case sqlparser.InStr:
		return &In{Left: left, Right: right}, nil
	case sqlparser.NotInStr:
		return &Not{Input: &In{Left: left, Right: right}}, nil
	case sqlparser.LikeStr:
		return &Like{Left: left, Right: right}, nil
	case sqlparser.NotLikeStr:
		return &Not{Input: &Like{Left: left, Right: right}}, nil
	case sqlparser.RegexpStr:
		return &Regexp{Left: left, Right: right}, nil
	case sqlparser.NotRegexpStr:
		return &Not{Input: &Regexp{Left: left, Right: right}}, nil
	case sqlparser.JSONExtractOp:
		return &JSONExtract{Left: left, Right: right}, nil
	case sqlparser.JSONUnquoteExtractOp:
		return &Convert{Input: &JSONExtract{Left: left, Right: right}, Type: &sqlparser.ConvertType{Type: querypb.Type_name[int32(querypb.Type_VARCHAR)]}}, nil
	default:
		return nil, fmt.Errorf("unsupported comparison operator %s", expr.Operator)
	}
}

func (p *Planner) planSQLVal(expr *sqlparser.SQLVal) (Expr, error) {
	switch expr.Type {
	case sqlparser.StrVal:
		return &Literal{Value: &querypb.BindVariable{Type: querypb.Type_VARCHAR, Value: expr.Val}}, nil
	case sqlparser.IntVal:
		return &Literal{Value: &querypb.BindVariable{Type: querypb.Type_INT64, Value: expr.Val}}, nil
	case sqlparser.FloatVal:
		return &Literal{Value: &querypb.BindVariable{Type: querypb.Type_FLOAT64, Value: expr.Val}}, nil
	case sqlparser.HexNum:
		i, err := strconv.ParseUint(string(expr.Val), 16, 64)
		if err != nil {
			return nil, err
		}
		_, data, err := Marshal(i)
		if err != nil {
			return nil, err
		}
		return &Literal{Value: &querypb.BindVariable{Type: querypb.Type_UINT64, Value: data}}, nil
	case sqlparser.HexVal:
		data, err := hex.DecodeString(string(expr.Val))
		if err != nil {
			return nil, err
		}
		return &Literal{Value: &querypb.BindVariable{Type: querypb.Type_VARBINARY, Value: data}}, nil
	case sqlparser.ValArg:
	case sqlparser.BitVal:
		data := new(big.Int)
		data, ok := data.SetString(string(expr.Val), 2)
		if !ok {
			return nil, fmt.Errorf("invalid bit string '%s'", expr.Val)
		}
		return &Literal{Value: &querypb.BindVariable{Type: querypb.Type_BIT, Value: data.Bytes()}}, nil

	}
	return nil, driver.ErrSkip
}

func (p *Planner) planNullValue(_ *sqlparser.NullVal) (Expr, error) {
	return &Literal{Value: NULL}, nil
}

func (p *Planner) planBoolVal(expr sqlparser.BoolVal) (Expr, error) {
	val := TRUE
	if expr {
		val = FALSE
	}
	return &Literal{Value: val}, nil
}

func (p *Planner) planColName(expr *sqlparser.ColName) (Expr, error) {
	return &Column{Value: expr}, nil
}
