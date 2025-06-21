package engine

import (
	"context"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type NOPPlan struct {
}

var _ Plan = (*NOPPlan)(nil)

func (p *NOPPlan) Run(_ context.Context, _ map[string]*querypb.BindVariable) (schema.Cursor, error) {
	return schema.NewInMemoryCursor(nil), nil
}

func (p *NOPPlan) Walk(f func(Plan) (bool, error)) (bool, error) {
	return f(p)
}

func (p *NOPPlan) String() string {
	return ""
}
