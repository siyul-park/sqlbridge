package task

import (
	"database/sql/driver"
	"errors"
	"sync"

	"github.com/xwb1989/sqlparser"
)

type Registry struct {
	builders []Builder
	mu       sync.RWMutex
}

var _ Builder = (*Registry)(nil)

func NewRegistry() *Registry {
	return &Registry{}
}

func (r *Registry) AddBuilder(builder Builder) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, b := range r.builders {
		if b == builder {
			return false
		}
	}
	r.builders = append(r.builders, builder)
	return true
}

func (r *Registry) RemoveBuilder(builder Builder) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i, b := range r.builders {
		if b == builder {
			r.builders = append(r.builders[:i], r.builders[i+1:]...)
			return true
		}
	}
	return false
}

func (r *Registry) Build(node sqlparser.SQLNode) (Task, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, builder := range r.builders {
		task, err := builder.Build(node)
		if err != nil {
			if errors.Is(err, driver.ErrSkip) {
				continue
			}
			return nil, err
		}
		return task, nil
	}
	return nil, driver.ErrSkip
}
