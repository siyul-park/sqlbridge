package vm

import (
	"context"
	"database/sql/driver"
	"github.com/siyul-park/sqlbridge/schema"
	"github.com/siyul-park/sqlbridge/task"
	"github.com/xwb1989/sqlparser"
)

type VM struct {
	builder task.Builder
	schema  schema.Schema
}

func WithBuilder(builder task.Builder) func(*VM) {
	return func(vm *VM) { vm.builder = builder }
}

func WithSchema(schema schema.Schema) func(*VM) {
	return func(vm *VM) { vm.schema = schema }
}

func New(opts ...func(*VM)) *VM {
	vm := &VM{}
	for _, opt := range opts {
		opt(vm)
	}
	if vm.builder == nil {
		vm.builder = task.Build(func(node sqlparser.SQLNode) (task.Task, error) { return nil, driver.ErrSkip })
	}
	if vm.schema == nil {
		vm.schema = schema.New(nil)
	}
	return vm
}

func (vm *VM) Exec(ctx context.Context, node sqlparser.SQLNode) (driver.Result, error) {
	val, err := vm.Eval(ctx, node)
	if err != nil {
		return nil, err
	}
	result, ok := val.(driver.Result)
	if !ok {
		return nil, driver.ErrSkip
	}
	return result, nil
}

func (vm *VM) Query(ctx context.Context, node sqlparser.SQLNode) (driver.Rows, error) {
	val, err := vm.Eval(ctx, node)
	if err != nil {
		return nil, err
	}
	rows, ok := val.(driver.Rows)
	if !ok {
		return nil, driver.ErrSkip
	}
	return rows, nil
}

func (vm *VM) Eval(ctx context.Context, node sqlparser.SQLNode) (driver.Value, error) {
	tsk, err := vm.builder.Build(node)
	if err != nil {
		return nil, err
	}
	return tsk.Run(ctx, vm.schema)
}
