package compile

import (
	"strconv"

	"github.com/pkg/errors"
	"github.com/siyul-park/minivm/instr"
	"github.com/siyul-park/minivm/interp"
	"github.com/siyul-park/minivm/program"
	"github.com/siyul-park/minivm/types"
	"github.com/xwb1989/sqlparser"

	"github.com/siyul-park/sqlbridge/catalog"
	"github.com/siyul-park/sqlbridge/runtime"
)

// ErrUnsupported indicates the statement or expression cannot yet be compiled.
var ErrUnsupported = errors.New("compile: unsupported construct")

// Program is a compiled SQL statement: a minivm program plus the output schema
// the driver needs to surface results.
type Program struct {
	Program *program.Program
	Columns []*sqlparser.ColName // output columns; nil for SELECT *
	Star    bool                 // true when projection is SELECT *
	Globals int                  // number of VM global slots required
}

// Compiler turns sqlparser statements into minivm programs against a catalog.
type Compiler struct {
	catalog catalog.Catalog
}

// New returns a Compiler bound to cat.
func New(cat catalog.Catalog) *Compiler {
	return &Compiler{catalog: cat}
}

// gen holds mutable state while emitting a single program.
type gen struct {
	cat    catalog.Catalog
	b      *Builder
	consts []types.Value
}

// konst appends a constant and returns its index.
func (g *gen) konst(v types.Value) uint64 {
	idx := uint64(len(g.consts))
	g.consts = append(g.consts, v)
	return idx
}

// host appends a host function constant and returns its index.
func (g *gen) host(fn *interp.HostFunction) uint64 {
	return g.konst(fn)
}

// Compile compiles a parsed statement.
func (c *Compiler) Compile(stmt sqlparser.Statement) (*Program, error) {
	switch s := stmt.(type) {
	case *sqlparser.Select:
		return c.compileSelect(s)
	default:
		return nil, errors.Wrapf(ErrUnsupported, "%T", stmt)
	}
}

func (c *Compiler) compileSelect(sel *sqlparser.Select) (*Program, error) {
	tbl, err := resolveTable(c.catalog, sel.From)
	if err != nil {
		return nil, err
	}

	star, columns, exprs, err := projection(sel.SelectExprs)
	if err != nil {
		return nil, err
	}

	g := &gen{cat: c.catalog, b: NewBuilder()}

	open := g.host(runtime.OpenFunc(tbl))
	next := g.host(runtime.NextFunc())

	// open the scan cursor into the session
	g.b.Emit(instr.CONST_GET, open).Emit(instr.CALL).Emit(instr.DROP)

	loop := g.b.Label()
	done := g.b.Label()

	g.b.Bind(loop)
	// if next() reports end of stream -> done
	g.b.Emit(instr.CONST_GET, next).Emit(instr.CALL).Emit(instr.I32_EQZ)
	g.b.Branch(instr.BR_IF, done)

	// WHERE: skip the row when the predicate is not true.
	if sel.Where != nil {
		if err := compileExpr(g, sel.Where.Expr); err != nil {
			return nil, err
		}
		truthy := g.host(runtime.TruthyFunc())
		g.b.Emit(instr.CONST_GET, truthy).Emit(instr.CALL).Emit(instr.I32_EQZ)
		g.b.Branch(instr.BR_IF, loop)
	}

	// Projection.
	if star {
		emit := g.host(runtime.EmitStarFunc())
		g.b.Emit(instr.CONST_GET, emit).Emit(instr.CALL).Emit(instr.DROP)
	} else {
		push := g.host(runtime.PushFunc())
		commit := g.host(runtime.CommitFunc())
		for _, e := range exprs {
			if err := compileExpr(g, e); err != nil {
				return nil, err
			}
			g.b.Emit(instr.CONST_GET, push).Emit(instr.CALL).Emit(instr.DROP)
		}
		g.b.Emit(instr.CONST_GET, commit).Emit(instr.CALL).Emit(instr.DROP)
	}

	g.b.Branch(instr.BR, loop)
	g.b.Bind(done)

	// LIMIT / OFFSET applied to the materialized result.
	if sel.Limit != nil {
		offset, count, err := limitBounds(sel.Limit)
		if err != nil {
			return nil, err
		}
		g.b.Emit(instr.CONST_GET, g.host(runtime.LimitFunc(offset, count))).Emit(instr.CALL).Emit(instr.DROP)
	}

	insts, err := g.b.Build()
	if err != nil {
		return nil, err
	}

	return &Program{
		Program: program.New(insts, program.WithConstants(g.consts...)),
		Columns: columns,
		Star:    star,
		Globals: runtime.GlobalCount,
	}, nil
}

// resolveTable extracts the single source table from a FROM clause.
func resolveTable(cat catalog.Catalog, from sqlparser.TableExprs) (catalog.Table, error) {
	if len(from) != 1 {
		return nil, errors.Wrap(ErrUnsupported, "exactly one table is supported")
	}
	aliased, ok := from[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, errors.Wrapf(ErrUnsupported, "from %T", from[0])
	}
	name, ok := aliased.Expr.(sqlparser.TableName)
	if !ok {
		return nil, errors.Wrapf(ErrUnsupported, "table expr %T", aliased.Expr)
	}
	return cat.Table(name.Name.String())
}

// projection classifies the select list into a star flag, output columns, and
// the expressions to evaluate per row.
func projection(exprs sqlparser.SelectExprs) (star bool, columns []*sqlparser.ColName, values []sqlparser.Expr, err error) {
	for _, se := range exprs {
		switch e := se.(type) {
		case *sqlparser.StarExpr:
			return true, nil, nil, nil
		case *sqlparser.AliasedExpr:
			columns = append(columns, outputColumn(e))
			values = append(values, e.Expr)
		default:
			return false, nil, nil, errors.Wrapf(ErrUnsupported, "select expr %T", se)
		}
	}
	return false, columns, values, nil
}

// limitBounds extracts the OFFSET and row count from a LIMIT clause. A row
// count of -1 means unbounded.
func limitBounds(limit *sqlparser.Limit) (offset, count int64, err error) {
	count = -1
	if limit.Rowcount != nil {
		if count, err = intLiteral(limit.Rowcount); err != nil {
			return 0, 0, err
		}
	}
	if limit.Offset != nil {
		if offset, err = intLiteral(limit.Offset); err != nil {
			return 0, 0, err
		}
	}
	return offset, count, nil
}

// intLiteral reads a non-negative integer literal expression.
func intLiteral(expr sqlparser.Expr) (int64, error) {
	v, ok := expr.(*sqlparser.SQLVal)
	if !ok || v.Type != sqlparser.IntVal {
		return 0, errors.Wrapf(ErrUnsupported, "limit expr %T", expr)
	}
	n, err := strconv.ParseInt(string(v.Val), 10, 64)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return n, nil
}

// outputColumn derives the result column name for an aliased select expression.
func outputColumn(e *sqlparser.AliasedExpr) *sqlparser.ColName {
	if !e.As.IsEmpty() {
		return &sqlparser.ColName{Name: e.As}
	}
	if col, ok := e.Expr.(*sqlparser.ColName); ok {
		return col
	}
	return &sqlparser.ColName{Name: sqlparser.NewColIdent(sqlparser.String(e.Expr))}
}
