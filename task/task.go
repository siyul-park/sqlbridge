package task

import (
	"context"
)

type Task interface {
	Run(ctx context.Context, value any) (any, error)
}

type Run func(ctx context.Context, value any) (any, error)

var _ Task = (Run)(nil)

func (f Run) Run(ctx context.Context, value any) (any, error) {
	return f(ctx, value)
}
