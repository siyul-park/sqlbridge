package eval

import (
	"context"
	"fmt"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type Func struct {
	Dispatcher *Dispatcher
	Qualifier  sqlparser.TableIdent
	Name       sqlparser.ColIdent
	Input      Expr
	Aggregate  bool
}

var _ Expr = (*Func)(nil)

func (e *Func) Eval(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
	name := e.Name.String()
	if !e.Qualifier.IsEmpty() {
		name = fmt.Sprintf("%s.%s", e.Qualifier.String(), name)
	}

	var rows []schema.Row
	if e.Aggregate && len(row.Children) > 0 {
		rows = row.Children
	} else {
		rows = []schema.Row{row}
	}

	args := make([]Value, 0, len(rows))
	for _, r := range rows {
		val, err := e.Input.Eval(ctx, r, binds)
		if err != nil {
			return nil, err
		}
		switch val := val.(type) {
		case *Tuple:
			args = append(args, val.Values()...)
		default:
			args = append(args, val)
		}
	}
	return e.Dispatcher.Dispatch(name, args)
}

func (e *Func) String() string {
	name := e.Name.String()
	if !e.Qualifier.IsEmpty() {
		name = fmt.Sprintf("%s.%s", e.Qualifier.String(), name)
	}
	return fmt.Sprintf("Func(%s, %s)", name, e.Input.String())
}
