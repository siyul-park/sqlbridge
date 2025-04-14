package eval

import (
	"context"
	"fmt"
	"strings"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

type Func struct {
	Dispatcher *Dispatcher
	Qualifier  sqlparser.TableIdent
	Name       sqlparser.ColIdent
	Distinct   bool
	Aggregate  bool
	Exprs      []Expr
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
		var vals []Value
		for _, expr := range e.Exprs {
			val, err := expr.Eval(ctx, r, binds)
			if err != nil {
				return nil, err
			}
			switch val := val.(type) {
			case *Tuple:
				vals = append(vals, val.Values()...)
			default:
				vals = append(vals, val)
			}
		}

		var val Value
		if len(vals) == 1 {
			val = vals[0]
		} else if len(vals) > 1 {
			val = NewTuple(vals)
		}

		duplicate := true
		for _, v := range args {
			cmp, err := Compare(v, val)
			if cmp == 0 && err == nil {
				duplicate = false
				break
			}
		}
		if !duplicate {
			continue
		}

		args = append(args, val)
	}

	return e.Dispatcher.Dispatch(name, args)
}

func (e *Func) String() string {
	name := e.Name.String()
	if !e.Qualifier.IsEmpty() {
		name = fmt.Sprintf("%s.%s", e.Qualifier.String(), name)
	}
	args := make([]string, len(e.Exprs))
	for i, arg := range e.Exprs {
		args[i] = arg.String()
	}
	return fmt.Sprintf("Func(%s, %v, %s)", name, e.Distinct, strings.Join(args, ", "))
}
