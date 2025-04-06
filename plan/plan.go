package plan

import (
	"github.com/xwb1989/sqlparser"
)

type Plan interface {
	Children() []Plan
}

type NopPlan struct{}

var _ Plan = (*NopPlan)(nil)

func (p *NopPlan) Children() []Plan {
	return nil
}

type ScanPlan struct {
	Table sqlparser.TableName
}

var _ Plan = (*ScanPlan)(nil)

func (p *ScanPlan) Children() []Plan {
	return nil
}

type AliasPlan struct {
	Input Plan
	Alias sqlparser.TableIdent
}

var _ Plan = (*AliasPlan)(nil)

func (p *AliasPlan) Children() []Plan {
	return []Plan{p.Input}
}

type JoinPlan struct {
	Left  Plan
	Right Plan
	Join  string
	On    sqlparser.Expr
	Using []sqlparser.ColIdent
}

var _ Plan = (*JoinPlan)(nil)

func (p *JoinPlan) Children() []Plan {
	return []Plan{p.Left, p.Right}
}

type FilterPlan struct {
	Input Plan
	Expr  sqlparser.Expr
}

var _ Plan = (*FilterPlan)(nil)

func (p *FilterPlan) Children() []Plan {
	return []Plan{p.Input}
}

type ProjectPlan struct {
	Input Plan
	Exprs sqlparser.SelectExprs
}

var _ Plan = (*ProjectPlan)(nil)

func (p *ProjectPlan) Children() []Plan {
	return []Plan{p.Input}
}

type GroupPlan struct {
	Input       Plan
	GroupExpr   sqlparser.GroupBy
	SelectExprs sqlparser.SelectExprs
}

var _ Plan = (*GroupPlan)(nil)

func (p *GroupPlan) Children() []Plan {
	return []Plan{p.Input}
}

type OrderPlan struct {
	Input  Plan
	Orders sqlparser.OrderBy
}

var _ Plan = (*OrderPlan)(nil)

func (p *OrderPlan) Children() []Plan {
	return []Plan{p.Input}
}

type LimitPlan struct {
	Input Plan
	Limit *sqlparser.Limit
}

var _ Plan = (*LimitPlan)(nil)

func (p *LimitPlan) Children() []Plan {
	return []Plan{p.Input}
}
