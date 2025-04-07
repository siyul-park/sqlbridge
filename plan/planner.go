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
		return &NopPlan{}, nil
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
		left = &JoinPlan{Left: left, Right: right, Join: sqlparser.JoinStr}
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

	plan := &JoinPlan{
		Left:  left,
		Right: right,
		Join:  node.Join,
	}

	if node.Condition.On != nil {
		plan.On = node.Condition.On
	}
	if node.Condition.Using != nil {
		plan.Using = node.Condition.Using
	}

	return plan, nil
}

func (p *Planner) planTableName(node sqlparser.TableName) (Plan, error) {
	return &ScanPlan{Table: node}, nil
}

func (p *Planner) planSubquery(node *sqlparser.Subquery) (Plan, error) {
	return p.planSelectStatement(node.Select)
}

func (p *Planner) planWhere(input Plan, node *sqlparser.Where) (Plan, error) {
	if node != nil {
		return &FilterPlan{
			Input: input,
			Expr:  node.Expr,
		}, nil
	}
	return input, nil
}

func (p *Planner) planGroupBy(input Plan, node sqlparser.GroupBy) (Plan, error) {
	if len(node) > 0 {
		return &GroupPlan{
			Input: input,
			Exprs: node,
		}, nil
	}
	return input, nil
}

func (p *Planner) planHaving(input Plan, node *sqlparser.Where) (Plan, error) {
	if node != nil {
		return &FilterPlan{
			Input: input,
			Expr:  node.Expr,
		}, nil
	}
	return input, nil
}

func (p *Planner) planSelectExprs(input Plan, node sqlparser.SelectExprs) (Plan, error) {
	if len(node) > 0 {
		return &ProjectPlan{
			Input: input,
			Exprs: node,
		}, nil
	}
	return input, nil
}

func (p *Planner) planOrderBy(input Plan, node sqlparser.OrderBy) (Plan, error) {
	if len(node) > 0 {
		return &OrderPlan{
			Input: input,
			Exprs: node,
		}, nil
	}
	return input, nil
}

func (p *Planner) planLimit(input Plan, node *sqlparser.Limit) (Plan, error) {
	if node != nil {
		return &LimitPlan{
			Input: input,
			Exprs: node,
		}, nil
	}
	return input, nil
}
