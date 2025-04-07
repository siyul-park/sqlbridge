package plan

import (
	"database/sql/driver"

	"github.com/xwb1989/sqlparser"
)

type Planner struct {
}

func NewPlanner() *Planner {
	return &Planner{}
}

func (p *Planner) Plan(node sqlparser.Statement) (Plan, error) {
	switch n := node.(type) {
	case *sqlparser.Union:
	case *sqlparser.Select:
		return p.planSelect(n)
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
	case *sqlparser.ParenSelect:
	case *sqlparser.Stream:
	}
	return nil, driver.ErrSkip
}

func (p *Planner) planSelectStatement(n sqlparser.SelectStatement) (Plan, error) {
	switch n := n.(type) {
	case *sqlparser.Union:
	case *sqlparser.Select:
		return p.planSelect(n)
	case *sqlparser.ParenSelect:
	}
	return nil, driver.ErrSkip

}

func (p *Planner) planSelect(n *sqlparser.Select) (Plan, error) {
	input, err := p.planTableExprs(n.From)
	if err != nil {
		return nil, err
	}

	if n.Where != nil {
		input = &FilterPlan{Input: input, Expr: n.Where.Expr}
	}

	if len(n.GroupBy) > 0 {
		input = &GroupPlan{
			Input: input,
			Exprs: n.GroupBy,
		}
	}

	if n.Having != nil {
		input = &FilterPlan{
			Input: input,
			Expr:  n.Having.Expr,
		}
	}

	if len(n.SelectExprs) > 0 {
		input = &ProjectPlan{
			Input: input,
			Exprs: n.SelectExprs,
		}
	}

	if len(n.OrderBy) > 0 {
		input = &OrderPlan{
			Input: input,
			Exprs: n.OrderBy,
		}
	}

	if n.Limit != nil {
		input = &LimitPlan{
			Input: input,
			Exprs: n.Limit,
		}
	}
	return input, nil
}

func (p *Planner) planTableExprs(n sqlparser.TableExprs) (Plan, error) {
	if len(n) == 0 {
		return &NopPlan{}, nil
	}

	left, err := p.planTableExpr(n[0])
	if err != nil {
		return nil, err
	}

	for _, expr := range n[1:] {
		right, err := p.planTableExpr(expr)
		if err != nil {
			return nil, err
		}
		left = &JoinPlan{Left: left, Right: right, Join: sqlparser.JoinStr}
	}
	return left, nil
}

func (p *Planner) planTableExpr(n sqlparser.TableExpr) (Plan, error) {
	switch n := n.(type) {
	case *sqlparser.AliasedTableExpr:
		return p.planAliasedTableExpr(n)
	case *sqlparser.ParenTableExpr:
		return p.planParenTableExpr(n)
	case *sqlparser.JoinTableExpr:
		return p.planJoinTableExpr(n)
	}
	return nil, driver.ErrSkip
}

func (p *Planner) planSimpleTableExpr(n sqlparser.SimpleTableExpr) (Plan, error) {
	switch n := n.(type) {
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

	alias := node.As
	if alias.IsEmpty() {
		switch expr := node.Expr.(type) {
		case sqlparser.TableName:
			alias = expr.Name
		default:
			alias = sqlparser.NewTableIdent(sqlparser.String(expr))
		}
	}

	return &AliasPlan{Input: plan, Alias: alias}, nil
}

func (p *Planner) planParenTableExpr(n *sqlparser.ParenTableExpr) (Plan, error) {
	return p.planTableExprs(n.Exprs)
}

func (p *Planner) planJoinTableExpr(n *sqlparser.JoinTableExpr) (Plan, error) {
	left, err := p.planTableExpr(n.LeftExpr)
	if err != nil {
		return nil, err
	}

	right, err := p.planTableExpr(n.RightExpr)
	if err != nil {
		return nil, err
	}

	plan := &JoinPlan{
		Left:  left,
		Right: right,
		Join:  n.Join,
	}

	if n.Condition.On != nil {
		plan.On = n.Condition.On
	}
	if n.Condition.Using != nil {
		plan.Using = n.Condition.Using
	}

	return plan, nil
}

func (p *Planner) planTableName(node sqlparser.TableName) (Plan, error) {
	return &ScanPlan{Table: node}, nil
}

func (p *Planner) planSubquery(n *sqlparser.Subquery) (Plan, error) {
	return p.planSelectStatement(n.Select)
}
