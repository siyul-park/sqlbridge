package plan

import (
	"context"
	"fmt"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type Scan struct {
	Catalog schema.Catalog
	Table   sqlparser.TableName
}

var _ Plan = (*Scan)(nil)

func (p *Scan) Run(ctx context.Context, _ map[string]*querypb.BindVariable) (schema.Cursor, error) {
	table, err := p.Catalog.Table(p.Table.Name.CompliantName())
	if err != nil {
		return nil, err
	}
	return table.Scan(ctx)
}

func (p *Scan) String() string {
	return fmt.Sprintf("Scan(%s)", sqlparser.String(p.Table))
}
