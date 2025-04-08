package task

import (
	"database/sql/driver"

	"github.com/siyul-park/sqlbridge/vm"

	"github.com/siyul-park/sqlbridge/schema"

	"github.com/siyul-park/sqlbridge/plan"
)

type Builder struct {
	vm      *vm.VM
	catalog schema.Catalog
}

type Option func(*Builder)

func WithVM(vm *vm.VM) Option {
	return func(b *Builder) { b.vm = vm }
}

func WithCatalog(catalog schema.Catalog) Option {
	return func(b *Builder) { b.catalog = catalog }
}

func NewBuilder(opts ...Option) *Builder {
	b := &Builder{
		vm:      vm.New(),
		catalog: schema.NewInMemoryCatalog(nil),
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

func (b *Builder) Build(p plan.Plan, args ...driver.NamedValue) (Task, error) {
	switch p := p.(type) {
	case *plan.NopPlan:
		return b.buildNopPlan(p)
	case *plan.ScanPlan:
		return b.buildScanPlan(p)
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

func (b *Builder) buildNopPlan(_ *plan.NopPlan) (Task, error) {
	return &NopTask{}, nil
}

func (b *Builder) buildScanPlan(p *plan.ScanPlan) (Task, error) {
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
	return &JoinTask{VM: b.vm, Left: left, Right: right, Join: p.Join, On: p.On, Using: p.Using, Args: args}, nil
}

func (b *Builder) buildFilterPlan(p *plan.FilterPlan, args ...driver.NamedValue) (Task, error) {
	input, err := b.Build(p.Input, args...)
	if err != nil {
		return nil, err
	}
	return &FilterTask{VM: b.vm, Input: input, Expr: p.Expr, Args: args}, nil
}

func (b *Builder) buildProjectPlan(p *plan.ProjectPlan, args ...driver.NamedValue) (Task, error) {
	input, err := b.Build(p.Input, args...)
	if err != nil {
		return nil, err
	}
	return &ProjectTask{VM: b.vm, Input: input, Exprs: p.Exprs, Args: args}, nil
}

func (b *Builder) buildGroupPlan(p *plan.GroupPlan, args ...driver.NamedValue) (Task, error) {
	input, err := b.Build(p.Input, args...)
	if err != nil {
		return nil, err
	}
	return &GroupTask{VM: b.vm, Input: input, Exprs: p.Exprs, Args: args}, nil
}

func (b *Builder) buildOrderPlan(p *plan.OrderPlan, args ...driver.NamedValue) (Task, error) {
	input, err := b.Build(p.Input, args...)
	if err != nil {
		return nil, err
	}
	return &OrderTask{VM: b.vm, Input: input, Exprs: p.Exprs, Args: args}, nil
}

func (b *Builder) buildLimitPlan(p *plan.LimitPlan, args ...driver.NamedValue) (Task, error) {
	input, err := b.Build(p.Input, args...)
	if err != nil {
		return nil, err
	}
	return &LimitTask{VM: b.vm, Input: input, Exprs: p.Exprs, Args: args}, nil
}
