package plan

import (
	"context"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type NOP struct {
}

var _ Plan = (*NOP)(nil)

func (p *NOP) Run(_ context.Context, _ map[string]*querypb.BindVariable) (schema.Cursor, error) {
	return schema.NewInMemoryCursor(nil), nil
}

func (p *NOP) String() string {
	return ""
}
