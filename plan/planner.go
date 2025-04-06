package plan

import (
	"database/sql/driver"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser"
)

type Planner struct {
	catalog schema.Catalog
}

func NewPlanner(catalog schema.Catalog) *Planner {
	return &Planner{
		catalog: catalog,
	}
}

func (p *Planner) Plan(node sqlparser.SQLNode) (Plan, error) {
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
	case sqlparser.Values:
	case *sqlparser.PartitionSpec:
	case *sqlparser.PartitionDefinition:
	case *sqlparser.TableSpec:
	case *sqlparser.ColumnDefinition:
	case *sqlparser.ColumnType:
	case *sqlparser.IndexDefinition:
	case *sqlparser.IndexInfo:
	case *sqlparser.VindexSpec:
	case *sqlparser.VindexParam:
	case sqlparser.SelectExprs:
	case *sqlparser.StarExpr:
	case *sqlparser.AliasedExpr:
	case *sqlparser.Nextval:
	case sqlparser.Columns:
	case sqlparser.Partitions:
	case sqlparser.TableExprs:
		return p.planTableExprs(n)
	case *sqlparser.AliasedTableExpr:
		return p.planAliasedTableExpr(n)
	case *sqlparser.ParenTableExpr:
		return p.planParenTableExpr(n)
	case *sqlparser.JoinTableExpr:
		return p.planJoinTableExpr(n)
	case sqlparser.TableName:
		return p.planTableName(n)
	case *sqlparser.Subquery:
	case sqlparser.TableNames:
	case *sqlparser.IndexHints:
	case *sqlparser.Where:
	case *sqlparser.AndExpr:
	case *sqlparser.OrExpr:
	case *sqlparser.NotExpr:
	case *sqlparser.ParenExpr:
	case *sqlparser.ComparisonExpr:
	case *sqlparser.RangeCond:
	case *sqlparser.IsExpr:
	case *sqlparser.ExistsExpr:
	case *sqlparser.SQLVal:
	case *sqlparser.NullVal:
	case sqlparser.BoolVal:
	case *sqlparser.ColName:
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
	case sqlparser.Exprs:
	case *sqlparser.ConvertType:
	case *sqlparser.When:
	case *sqlparser.GroupBy:
	case *sqlparser.OrderBy:
	case *sqlparser.Order:
	case *sqlparser.Limit:
	case sqlparser.UpdateExprs:
	case *sqlparser.UpdateExpr:
	case sqlparser.SetExprs:
	case *sqlparser.SetExpr:
	case sqlparser.OnDup:
	case sqlparser.ColIdent:
	case sqlparser.TableIdent:
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
			Input:       input,
			GroupExpr:   n.GroupBy,
			SelectExprs: n.SelectExprs,
		}

		if n.Having != nil {
			input = &FilterPlan{
				Input: input,
				Expr:  n.Having.Expr,
			}
		}
	} else if len(n.SelectExprs) > 0 {
		input = &ProjectPlan{
			Input: input,
			Exprs: n.SelectExprs,
		}
	}

	if len(n.OrderBy) > 0 {
		input = &OrderPlan{
			Input:  input,
			Orders: n.OrderBy,
		}
	}

	if n.Limit != nil {
		input = &LimitPlan{
			Input: input,
			Limit: n.Limit,
		}
	}
	return input, nil
}

func (p *Planner) planTableExprs(n sqlparser.TableExprs) (Plan, error) {
	if len(n) == 0 {
		return &NopPlan{}, nil
	}
	if len(n) == 1 {
		return p.Plan(n[0])
	}

	left, err := p.Plan(n[0])
	if err != nil {
		return nil, err
	}

	for _, expr := range n[1:] {
		right, err := p.Plan(expr)
		if err != nil {
			return nil, err
		}
		left = &JoinPlan{Left: left, Right: right}
	}
	return left, nil
}

func (p *Planner) planAliasedTableExpr(node *sqlparser.AliasedTableExpr) (Plan, error) {
	plan, err := p.Plan(node.Expr)
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
	left, err := p.Plan(n.LeftExpr)
	if err != nil {
		return nil, err
	}

	right, err := p.Plan(n.RightExpr)
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
		// TODO: handle USING condition
	}

	return plan, nil
}

func (p *Planner) planTableName(node sqlparser.TableName) (Plan, error) {
	table, err := p.catalog.Table(node.Name.CompliantName())
	if err != nil {
		return nil, err
	}
	return &ScanPlan{Table: table}, nil
}
