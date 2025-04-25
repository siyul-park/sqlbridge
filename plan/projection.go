package plan

import (
	"context"
	"fmt"
	"strings"

	"github.com/siyul-park/sqlbridge/eval"
	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/querypb"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
)

type Projection struct {
	Input Plan
	Items []ProjectionItem
}

type ProjectionItem interface {
	iProjectionItem()
	String() string
}

type StartItem struct {
	Table sqlparser.TableName
}

type AliasItem struct {
	Expr eval.Expr
	As   sqlparser.ColIdent
}

var _ Plan = (*Projection)(nil)
var _ ProjectionItem = (*StartItem)(nil)
var _ ProjectionItem = (*AliasItem)(nil)

func (p *Projection) Run(ctx context.Context, binds map[string]*querypb.BindVariable) (schema.Cursor, error) {
	input, err := p.Input.Run(ctx, binds)
	if err != nil {
		return nil, err
	}
	return schema.NewMappedCursor(input, func(row schema.Row) (schema.Row, error) {
		var columns []*sqlparser.ColName
		var values []sqltypes.Value
		for _, term := range p.Items {
			switch term := term.(type) {
			case *StartItem:
				for i := 0; i < len(row.Columns); i++ {
					col := &sqlparser.ColName{Name: row.Columns[i].Name}
					val := row.Values[i]
					if !term.Table.IsEmpty() && row.Columns[i].Qualifier != term.Table {
						continue
					}
					columns = append(columns, col)
					values = append(values, val)
				}
			case *AliasItem:
				val, err := term.Expr.Eval(ctx, row, binds)
				if err != nil {
					return schema.Row{}, err
				}
				v, err := eval.ToSQL(val, val.Type())
				if err != nil {
					return schema.Row{}, err
				}
				columns = append(columns, &sqlparser.ColName{Name: term.As})
				values = append(values, v)
			}
		}
		row.Columns = columns
		row.Values = values
		return row, nil
	}), nil
}

func (p *Projection) String() string {
	var builder strings.Builder
	builder.WriteString("Table(")
	builder.WriteString(p.Input.String())
	for _, term := range p.Items {
		builder.WriteString(", ")
		builder.WriteString(term.String())
	}
	builder.WriteString(")")
	return builder.String()
}

func (*StartItem) iProjectionItem() {
}

func (t *StartItem) String() string {
	return fmt.Sprintf("Start(%s)", sqlparser.String(t.Table))
}

func (*AliasItem) iProjectionItem() {
}

func (t *AliasItem) String() string {
	return fmt.Sprintf("Alias(%s, %s)", t.Expr.String(), sqlparser.String(t.As))
}
