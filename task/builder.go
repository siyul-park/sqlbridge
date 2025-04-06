package task

import (
	"database/sql/driver"
	"github.com/siyul-park/sqlbridge/plan"
)

type Builder struct {
}

func NewBuilder() *Builder {
	return &Builder{}
}

func (b *Builder) Build(p plan.Plan) (Task, error) {
	switch p := p.(type) {
	case *plan.NopPlan:
		return b.buildNopPlan(p)
	case *plan.ScanPlan:
		return b.buildScanPlan(p)
	case *plan.AliasPlan:
		return b.buildAliasPlan(p)
	case *plan.JoinPlan:
		return b.buildJoinPlan(p)
	case *plan.FilterPlan:
		return b.buildFilterPlan(p)
	default:
		return nil, driver.ErrSkip
	}
}

func (b *Builder) buildNopPlan(_ *plan.NopPlan) (Task, error) {
	return &NopTask{}, nil
}

func (b *Builder) buildScanPlan(p *plan.ScanPlan) (Task, error) {
	return &ScanTask{Table: p.Table}, nil
}

func (b *Builder) buildAliasPlan(p *plan.AliasPlan) (Task, error) {
	input, err := b.Build(p.Input)
	if err != nil {
		return nil, err
	}
	return &AliasTask{Input: input, Alias: p.Alias}, nil
}

func (b *Builder) buildJoinPlan(p *plan.JoinPlan) (Task, error) {
	left, err := b.Build(p.Left)
	if err != nil {
		return nil, err
	}
	right, err := b.Build(p.Right)
	if err != nil {
		return nil, err
	}
	return &JoinTask{Left: left, Right: right, Join: p.Join, On: p.On, Using: p.Using}, nil
}

func (b *Builder) buildFilterPlan(p *plan.FilterPlan) (Task, error) {
	input, err := b.Build(p.Input)
	if err != nil {
		return nil, err
	}
	return &FilterTask{Input: input, Expr: p.Expr}, nil
}
