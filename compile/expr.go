package compile

import (
	"strconv"

	"github.com/pkg/errors"
	"github.com/siyul-park/minivm/instr"
	"github.com/xwb1989/sqlparser"

	"github.com/siyul-park/sqlbridge/function"
	"github.com/siyul-park/sqlbridge/runtime"
	"github.com/siyul-park/sqlbridge/value"
)

// scalarArgs flattens a scalar function's argument list into expressions.
func scalarArgs(exprs sqlparser.SelectExprs) ([]sqlparser.Expr, error) {
	args := make([]sqlparser.Expr, 0, len(exprs))
	for _, se := range exprs {
		ae, ok := se.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, errors.Wrapf(ErrUnsupported, "function argument %T", se)
		}
		args = append(args, ae.Expr)
	}
	return args, nil
}

// compileExpr emits bytecode that leaves the expression's value on the stack.
// The current row is expected in the GlobalRow slot.
func compileExpr(g *gen, expr sqlparser.Expr) error {
	switch e := expr.(type) {
	case *sqlparser.ParenExpr:
		return compileExpr(g, e.Expr)

	case *sqlparser.ColName:
		col := g.host(runtime.ColumnFunc(e))
		g.b.Emit(instr.CONST_GET, col).Emit(instr.CALL)
		return nil

	case *sqlparser.SQLVal:
		v, err := literal(e)
		if err != nil {
			return err
		}
		g.b.Emit(instr.CONST_GET, g.konst(runtime.Constant(v)))
		return nil

	case *sqlparser.NullVal:
		g.b.Emit(instr.CONST_GET, g.konst(runtime.Constant(nil)))
		return nil

	case *sqlparser.ComparisonExpr:
		if err := compileExpr(g, e.Left); err != nil {
			return err
		}
		if err := compileExpr(g, e.Right); err != nil {
			return err
		}
		g.b.Emit(instr.CONST_GET, g.host(runtime.CompareFunc(e.Operator))).Emit(instr.CALL)
		return nil

	case *sqlparser.BinaryExpr:
		if err := compileExpr(g, e.Left); err != nil {
			return err
		}
		if err := compileExpr(g, e.Right); err != nil {
			return err
		}
		g.b.Emit(instr.CONST_GET, g.host(runtime.ArithmeticFunc(e.Operator))).Emit(instr.CALL)
		return nil

	case *sqlparser.AndExpr:
		if err := compileExpr(g, e.Left); err != nil {
			return err
		}
		if err := compileExpr(g, e.Right); err != nil {
			return err
		}
		g.b.Emit(instr.CONST_GET, g.host(runtime.AndFunc())).Emit(instr.CALL)
		return nil

	case *sqlparser.OrExpr:
		if err := compileExpr(g, e.Left); err != nil {
			return err
		}
		if err := compileExpr(g, e.Right); err != nil {
			return err
		}
		g.b.Emit(instr.CONST_GET, g.host(runtime.OrFunc())).Emit(instr.CALL)
		return nil

	case *sqlparser.NotExpr:
		if err := compileExpr(g, e.Expr); err != nil {
			return err
		}
		g.b.Emit(instr.CONST_GET, g.host(runtime.NotFunc())).Emit(instr.CALL)
		return nil

	case *sqlparser.FuncExpr:
		name := e.Name.Lowered()
		if function.IsAggregate(name) {
			return errors.Wrapf(ErrUnsupported, "aggregate %q requires GROUP BY context", name)
		}
		args, err := scalarArgs(e.Exprs)
		if err != nil {
			return err
		}
		for _, a := range args {
			if err := compileExpr(g, a); err != nil {
				return err
			}
		}
		g.b.Emit(instr.CONST_GET, g.host(runtime.DispatchFunc(g.dispatcher, name, len(args)))).Emit(instr.CALL)
		return nil

	default:
		return errors.Wrapf(ErrUnsupported, "expr %T", expr)
	}
}

// literal converts a parsed SQL literal into a SQL value.
func literal(v *sqlparser.SQLVal) (value.Value, error) {
	switch v.Type {
	case sqlparser.IntVal:
		n, err := strconv.ParseInt(string(v.Val), 10, 64)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return value.NewInt64(n), nil
	case sqlparser.FloatVal:
		f, err := strconv.ParseFloat(string(v.Val), 64)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return value.NewFloat64(f), nil
	case sqlparser.StrVal:
		return value.NewVarChar(string(v.Val)), nil
	case sqlparser.HexVal, sqlparser.BitVal, sqlparser.HexNum:
		return value.NewVarBinary(v.Val), nil
	default:
		return nil, errors.Wrapf(ErrUnsupported, "literal type %v", v.Type)
	}
}
