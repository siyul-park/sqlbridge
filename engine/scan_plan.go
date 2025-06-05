package engine

import (
	"context"
	"fmt"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type ScanPlan struct {
	Catalog schema.Catalog
	Table   sqlparser.TableName
}

var _ Plan = (*ScanPlan)(nil)

func (p *ScanPlan) Run(ctx context.Context, _ map[string]*querypb.BindVariable) (schema.Cursor, error) {
	table, err := p.Catalog.Table(p.Table.Name.CompliantName())
	if err != nil {
		return nil, err
	}
	return table.Scan(ctx)
}

func (p *ScanPlan) String() string {
	return fmt.Sprintf("ScanPlan(%s)", sqlparser.String(p.Table))
}
