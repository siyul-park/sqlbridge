package plan

import (
	"database/sql/driver"

	"github.com/siyul-park/sqlbridge/eval"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser"
)

type Planner struct {
	catalog schema.Catalog
	builder *eval.Builder
}

func NewPlanner(catalog schema.Catalog, dispatcher *eval.Dispatcher) *Planner {
	return &Planner{
		catalog: catalog,
		builder: eval.NewBuilder(dispatcher),
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
		expr, err := p.builder.Build(node.Condition.On)
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
		expr, err := p.builder.Build(node.Expr)
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
		expr, err := p.builder.Build(node.Expr)
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
				expr, err := p.builder.Build(e.Expr)
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
