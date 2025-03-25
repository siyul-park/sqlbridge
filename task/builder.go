package task

import "github.com/xwb1989/sqlparser"

type Builder interface {
	Build(node sqlparser.SQLNode) (Task, error)
}

type builder struct {
	build func(node sqlparser.SQLNode) (Task, error)
}

var _ Builder = (*builder)(nil)

func Build(build func(node sqlparser.SQLNode) (Task, error)) Builder {
	return &builder{build: build}
}

func (b *builder) Build(node sqlparser.SQLNode) (Task, error) {
	return b.build(node)
}
