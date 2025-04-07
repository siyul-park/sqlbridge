package task

import (
	"database/sql/driver"

	"github.com/siyul-park/sqlbridge/vm"

	"github.com/siyul-park/sqlbridge/schema"

	"github.com/siyul-park/sqlbridge/plan"
)

type Builder struct {
	catalog schema.Catalog
}

func NewBuilder(catalog schema.Catalog) *Builder {
	return &Builder{catalog: catalog}
}

func (b *Builder) Build(p plan.Plan, args ...driver.NamedValue) (Task, error) {
	switch p := p.(type) {
	case *plan.NopPlan:
		return b.buildNopPlan(p, args...)
	case *plan.ScanPlan:
		return b.buildScanPlan(p, args...)
	case *plan.AliasPlan:
		return b.buildAliasPlan(p, args...)
	case *plan.JoinPlan:
		return b.buildJoinPlan(p, args...)
	case *plan.FilterPlan:
		return b.buildFilterPlan(p, args...)
	case *plan.ProjectPlan:
		return b.buildProjectPlan(p, args...)
	case *plan.GroupPlan:
		return b.buildGroupPlan(p, args...)
	case *plan.OrderPlan:
		return b.buildOrderPlan(p, args...)
	case *plan.LimitPlan:
		return b.buildLimitPlan(p, args...)
	default:
		return nil, driver.ErrSkip
	}
}

func (b *Builder) buildNopPlan(_ *plan.NopPlan, _ ...driver.NamedValue) (Task, error) {
	return &NopTask{}, nil
}

func (b *Builder) buildScanPlan(p *plan.ScanPlan, _ ...driver.NamedValue) (Task, error) {
	return &ScanTask{Catalog: b.catalog, Table: p.Table}, nil
}

func (b *Builder) buildAliasPlan(p *plan.AliasPlan, args ...driver.NamedValue) (Task, error) {
	input, err := b.Build(p.Input, args...)
	if err != nil {
		return nil, err
	}
	return &AliasTask{Input: input, Alias: p.Alias}, nil
}

func (b *Builder) buildJoinPlan(p *plan.JoinPlan, args ...driver.NamedValue) (Task, error) {
	left, err := b.Build(p.Left, args...)
	if err != nil {
		return nil, err
	}
	right, err := b.Build(p.Right, args...)
	if err != nil {
		return nil, err
	}
	return &JoinTask{VM: vm.New(args...), Left: left, Right: right, Join: p.Join, On: p.On, Using: p.Using}, nil
}

func (b *Builder) buildFilterPlan(p *plan.FilterPlan, args ...driver.NamedValue) (Task, error) {
	input, err := b.Build(p.Input, args...)
	if err != nil {
		return nil, err
	}
	return &FilterTask{VM: vm.New(args...), Input: input, Expr: p.Expr}, nil
}

func (b *Builder) buildProjectPlan(p *plan.ProjectPlan, args ...driver.NamedValue) (Task, error) {
	input, err := b.Build(p.Input, args...)
	if err != nil {
		return nil, err
	}
	return &ProjectTask{VM: vm.New(args...), Input: input, Exprs: p.Exprs}, nil
}

func (b *Builder) buildGroupPlan(p *plan.GroupPlan, args ...driver.NamedValue) (Task, error) {
	input, err := b.Build(p.Input, args...)
	if err != nil {
		return nil, err
	}
	return &GroupTask{VM: vm.New(args...), Input: input, Exprs: p.Exprs}, nil
}

func (b *Builder) buildOrderPlan(p *plan.OrderPlan, args ...driver.NamedValue) (Task, error) {
	input, err := b.Build(p.Input, args...)
	if err != nil {
		return nil, err
	}
	return &OrderTask{VM: vm.New(args...), Input: input, Orders: p.Orders}, nil
}

func (b *Builder) buildLimitPlan(p *plan.LimitPlan, args ...driver.NamedValue) (Task, error) {
	input, err := b.Build(p.Input, args...)
	if err != nil {
		return nil, err
	}
	return &LimitTask{VM: vm.New(args...), Input: input, Limit: p.Limit}, nil
}
