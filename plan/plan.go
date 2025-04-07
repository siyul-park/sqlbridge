package plan

import (
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
)

type Plan interface {
	Children() []Plan
	String() string
}

type NopPlan struct{}

var _ Plan = (*NopPlan)(nil)

func (p *NopPlan) Children() []Plan {
	return nil
}

func (p *NopPlan) String() string {
	return ""
}

type ScanPlan struct {
	Table sqlparser.TableName
}

var _ Plan = (*ScanPlan)(nil)

func (p *ScanPlan) Children() []Plan {
	return nil
}

func (p *ScanPlan) String() string {
	return fmt.Sprintf("Scan(%s)", p.Table)
}

type AliasPlan struct {
	Input Plan
	Alias sqlparser.TableIdent
}

var _ Plan = (*AliasPlan)(nil)

func (p *AliasPlan) String() string {
	return fmt.Sprintf("Alias(%s, %s)", p.Input.String(), p.Alias.String())
}

func (p *AliasPlan) Children() []Plan {
	return []Plan{p.Input}
}

type JoinPlan struct {
	Left  Plan
	Right Plan
	Join  string
	On    sqlparser.Expr
	Using sqlparser.Columns
}

var _ Plan = (*JoinPlan)(nil)

func (p *JoinPlan) Children() []Plan {
	return []Plan{p.Left, p.Right}
}

func (p *JoinPlan) String() string {
	var buf strings.Builder

	buf.WriteString("Join(")
	buf.WriteString(p.Left.String())
	buf.WriteString(", ")
	buf.WriteString(p.Right.String())
	buf.WriteString(", ")
	buf.WriteString(p.Join)

	if p.On != nil {
		buf.WriteString(", ")
		buf.WriteString(sqlparser.String(p.On))
	}
	if p.Using != nil {
		buf.WriteString(", ")
		buf.WriteString(sqlparser.String(p.Using))
	}

	buf.WriteString(")")
	return buf.String()
}

type FilterPlan struct {
	Input Plan
	Expr  sqlparser.Expr
}

var _ Plan = (*FilterPlan)(nil)

func (p *FilterPlan) Children() []Plan {
	return []Plan{p.Input}
}

func (p *FilterPlan) String() string {
	return fmt.Sprintf("Filter(%s, %s)", p.Input.String(), sqlparser.String(p.Expr))
}

type ProjectPlan struct {
	Input Plan
	Exprs sqlparser.SelectExprs
}

var _ Plan = (*ProjectPlan)(nil)

func (p *ProjectPlan) Children() []Plan {
	return []Plan{p.Input}
}

func (p *ProjectPlan) String() string {
	return fmt.Sprintf("Project(%s, %s)", p.Input.String(), sqlparser.String(p.Exprs))
}

type GroupPlan struct {
	Input Plan
	Exprs sqlparser.GroupBy
}

var _ Plan = (*GroupPlan)(nil)

func (p *GroupPlan) Children() []Plan {
	return []Plan{p.Input}
}

func (p *GroupPlan) String() string {
	return fmt.Sprintf("Group(%s,%s)", p.Input.String(), sqlparser.String(p.Exprs))
}

type OrderPlan struct {
	Input Plan
	Exprs sqlparser.OrderBy
}

var _ Plan = (*OrderPlan)(nil)

func (p *OrderPlan) Children() []Plan {
	return []Plan{p.Input}
}

func (p *OrderPlan) String() string {
	return fmt.Sprintf("Order(%s,%s)", p.Input.String(), sqlparser.String(p.Exprs))
}

type LimitPlan struct {
	Input Plan
	Exprs *sqlparser.Limit
}

var _ Plan = (*LimitPlan)(nil)

func (p *LimitPlan) Children() []Plan {
	return []Plan{p.Input}
}

func (p *LimitPlan) String() string {
	return fmt.Sprintf("Limit(%s,%s)", p.Input.String(), sqlparser.String(p.Exprs))
}
