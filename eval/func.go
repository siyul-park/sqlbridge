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
	Args       []FuncArg
}

type FuncArg interface {
	iFuncArg()
	String() string
}

type StartArg struct {
	Table sqlparser.TableName
}

type AliasArg struct {
	Expr Expr
	As   sqlparser.ColIdent
}

var _ Expr = (*Func)(nil)
var _ FuncArg = (*StartArg)(nil)
var _ FuncArg = (*AliasArg)(nil)

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
		vals, err := e.evalArgs(ctx, r, binds)
		if err != nil {
			return nil, err
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
	args := make([]string, len(e.Args))
	for i, arg := range e.Args {
		args[i] = arg.String()
	}
	return fmt.Sprintf("Func(%s, %v, %s)", name, e.Distinct, strings.Join(args, ", "))
}

func (e *Func) evalArgs(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) ([]Value, error) {
	var vals []Value
	for _, arg := range e.Args {
		switch a := arg.(type) {
		case *AliasArg:
			val, err := a.Expr.Eval(ctx, row, binds)
			if err != nil {
				return nil, err
			}
			vals = append(vals, val)
		case *StartArg:
			for i, col := range row.Columns {
				if !e.Qualifier.IsEmpty() && col.Qualifier.Name != e.Qualifier {
					continue
				}
				val, err := FromSQL(row.Values[i])
				if err != nil {
					return nil, err
				}
				vals = append(vals, val)
			}
		default:
			return nil, fmt.Errorf("unknown FuncArg type: %T", a)
		}
	}
	return vals, nil
}

func (*StartArg) iFuncArg() {}

func (s *StartArg) String() string {
	return fmt.Sprintf("Start(%s)", sqlparser.String(s.Table))
}

func (a *AliasArg) String() string {
	return fmt.Sprintf("Alias(%s, %s)", a.Expr.String(), a.As.String())
}

func (*AliasArg) iFuncArg() {}
