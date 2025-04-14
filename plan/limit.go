package plan

import (
	"context"
	"fmt"
	"io"

	"github.com/siyul-park/sqlbridge/eval"
	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type Limit struct {
	Input  Plan
	Offset eval.Expr
	Count  eval.Expr
}

var _ Plan = (*Limit)(nil)

func (p *Limit) Run(ctx context.Context, binds map[string]*querypb.BindVariable) (schema.Cursor, error) {
	input, err := p.Input.Run(ctx, binds)
	if err != nil {
		return nil, err
	}

	var offset int64
	if p.Offset != nil {
		val, err := p.Offset.Eval(ctx, schema.Row{}, binds)
		if err != nil {
			return nil, err
		}
		offset, err = eval.ToInt(val)
		if err != nil {
			return nil, err
		}
	}
	count := int64(-1)
	if p.Count != nil {
		val, err := p.Count.Eval(ctx, schema.Row{}, binds)
		if err != nil {
			return nil, err
		}
		count, err = eval.ToInt(val)
		if err != nil {
			return nil, err
		}
	}

	return schema.NewMappedCursor(input, func(row schema.Row) (schema.Row, error) {
		if offset > 0 {
			offset--
			return schema.Row{}, nil
		}
		if count == 0 {
			return schema.Row{}, io.EOF
		}
		count--
		return row, nil
	}), nil
}

func (p *Limit) String() string {
	return fmt.Sprintf("Limit(%s, %s)", p.Count.String(), p.Offset.String())
}
