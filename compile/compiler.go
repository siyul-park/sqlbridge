package compile

import (
	"strconv"

	"github.com/pkg/errors"
	"github.com/siyul-park/minivm/instr"
	"github.com/siyul-park/minivm/interp"
	"github.com/siyul-park/minivm/optimize"
	"github.com/siyul-park/minivm/program"
	"github.com/siyul-park/minivm/types"
	"github.com/xwb1989/sqlparser"

	"github.com/siyul-park/sqlbridge/catalog"
	"github.com/siyul-park/sqlbridge/function"
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
	catalog    catalog.Catalog
	dispatcher *function.Dispatcher
	optimizer  *optimize.Optimizer
}

// Option configures a Compiler.
type Option func(*Compiler)

// WithDispatcher overrides the SQL function dispatcher.
func WithDispatcher(d *function.Dispatcher) Option {
	return func(c *Compiler) { c.dispatcher = d }
}

// WithOptimizer sets the bytecode optimization level (optimize.O0 disables it).
func WithOptimizer(level optimize.Level) Option {
	return func(c *Compiler) { c.optimizer = optimize.NewOptimizer(level) }
}

// New returns a Compiler bound to cat. By default it uses the builtin functions.
// Bytecode optimization is opt-in via WithOptimizer: the current O1 passes do
// not preserve the byte-relative branch offsets these branch-heavy programs
// rely on, so optimization is disabled unless explicitly requested.
func New(cat catalog.Catalog, opts ...Option) *Compiler {
	c := &Compiler{
		catalog:    cat,
		dispatcher: function.New(function.WithBuiltin()),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// build finalizes the emitted instructions into an optimized program.
func (c *Compiler) build(g *gen) (*program.Program, error) {
	insts, err := g.b.Build()
	if err != nil {
		return nil, err
	}
	prog := program.New(insts, program.WithConstants(g.consts...))
	if c.optimizer != nil {
		return c.optimizer.Optimize(prog)
	}
	return prog, nil
}

// gen holds mutable state while emitting a single program.
type gen struct {
	cat        catalog.Catalog
	dispatcher *function.Dispatcher
	b          *Builder
	consts     []types.Value
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
	case *sqlparser.Insert:
		return c.compileInsert(s)
	case *sqlparser.Update:
		return c.compileUpdate(s)
	case *sqlparser.Delete:
		return c.compileDelete(s)
	default:
		return nil, errors.Wrapf(ErrUnsupported, "%T", stmt)
	}
}

func (c *Compiler) compileSelect(sel *sqlparser.Select) (*Program, error) {
	tbl, err := resolveTable(c.catalog, sel.From)
	if err != nil {
		return nil, err
	}

	if isAggregateQuery(sel.SelectExprs, sel.GroupBy) {
		return c.compileAggregate(sel, tbl)
	}

	star, columns, exprs, err := projection(sel.SelectExprs)
	if err != nil {
		return nil, err
	}

	g := &gen{cat: c.catalog, dispatcher: c.dispatcher, b: NewBuilder()}

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
	// Evaluate ORDER BY keys for the current row before it is committed.
	emitKeys := func() error {
		for _, ob := range sel.OrderBy {
			if err := compileExpr(g, ob.Expr); err != nil {
				return err
			}
			g.b.Emit(instr.CONST_GET, g.host(runtime.PushKeyFunc())).Emit(instr.CALL).Emit(instr.DROP)
		}
		return nil
	}

	if star {
		emit := g.host(runtime.EmitStarFunc())
		if err := emitKeys(); err != nil {
			return nil, err
		}
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
		if err := emitKeys(); err != nil {
			return nil, err
		}
		g.b.Emit(instr.CONST_GET, commit).Emit(instr.CALL).Emit(instr.DROP)
	}

	g.b.Branch(instr.BR, loop)
	g.b.Bind(done)

	// ORDER BY applied to the materialized result.
	if len(sel.OrderBy) > 0 {
		desc := make([]bool, len(sel.OrderBy))
		for i, ob := range sel.OrderBy {
			desc[i] = ob.Direction == sqlparser.DescScr
		}
		g.b.Emit(instr.CONST_GET, g.host(runtime.SortFunc(desc))).Emit(instr.CALL).Emit(instr.DROP)
	}

	// LIMIT / OFFSET applied to the materialized result.
	if sel.Limit != nil {
		offset, count, err := limitBounds(sel.Limit)
		if err != nil {
			return nil, err
		}
		g.b.Emit(instr.CONST_GET, g.host(runtime.LimitFunc(offset, count))).Emit(instr.CALL).Emit(instr.DROP)
	}

	prog, err := c.build(g)
	if err != nil {
		return nil, err
	}

	return &Program{
		Program: prog,
		Columns: columns,
		Star:    star,
		Globals: runtime.GlobalCount,
	}, nil
}

// compileAggregate compiles a GROUP BY / aggregate query: each row feeds the
// host-side grouper, which finalizes one output row per group after the scan.
func (c *Compiler) compileAggregate(sel *sqlparser.Select, tbl catalog.Table) (*Program, error) {
	slots, slotExprs, columns, err := aggregateSlots(sel.SelectExprs)
	if err != nil {
		return nil, err
	}
	groupExprs := []sqlparser.Expr(sel.GroupBy)

	spec := &runtime.GrouperSpec{
		Columns:    columns,
		Slots:      slots,
		GroupCount: len(groupExprs),
		Dispatcher: c.dispatcher,
	}

	g := &gen{cat: c.catalog, dispatcher: c.dispatcher, b: NewBuilder()}

	open := g.host(runtime.OpenFunc(tbl))
	next := g.host(runtime.NextFunc())
	groupRow := g.host(runtime.GroupRowFunc(len(groupExprs) + len(slots)))

	g.b.Emit(instr.CONST_GET, open).Emit(instr.CALL).Emit(instr.DROP)
	g.b.Emit(instr.CONST_GET, g.host(runtime.GroupInitFunc(spec))).Emit(instr.CALL).Emit(instr.DROP)

	loop := g.b.Label()
	done := g.b.Label()

	g.b.Bind(loop)
	g.b.Emit(instr.CONST_GET, next).Emit(instr.CALL).Emit(instr.I32_EQZ)
	g.b.Branch(instr.BR_IF, done)

	if sel.Where != nil {
		if err := compileExpr(g, sel.Where.Expr); err != nil {
			return nil, err
		}
		g.b.Emit(instr.CONST_GET, g.host(runtime.TruthyFunc())).Emit(instr.CALL).Emit(instr.I32_EQZ)
		g.b.Branch(instr.BR_IF, loop)
	}

	// Push group-identity values followed by each slot's value, then record.
	for _, ge := range groupExprs {
		if err := compileExpr(g, ge); err != nil {
			return nil, err
		}
	}
	for _, se := range slotExprs {
		if err := compileExpr(g, se); err != nil {
			return nil, err
		}
	}
	g.b.Emit(instr.CONST_GET, groupRow).Emit(instr.CALL).Emit(instr.DROP)

	g.b.Branch(instr.BR, loop)
	g.b.Bind(done)

	g.b.Emit(instr.CONST_GET, g.host(runtime.GroupFinalizeFunc())).Emit(instr.CALL).Emit(instr.DROP)

	if sel.Limit != nil {
		offset, count, err := limitBounds(sel.Limit)
		if err != nil {
			return nil, err
		}
		g.b.Emit(instr.CONST_GET, g.host(runtime.LimitFunc(offset, count))).Emit(instr.CALL).Emit(instr.DROP)
	}

	prog, err := c.build(g)
	if err != nil {
		return nil, err
	}

	return &Program{
		Program: prog,
		Columns: columns,
		Globals: runtime.GlobalCount,
	}, nil
}

// isAggregateQuery reports whether the select list or a GROUP BY clause require
// aggregation.
func isAggregateQuery(exprs sqlparser.SelectExprs, groupBy sqlparser.GroupBy) bool {
	if len(groupBy) > 0 {
		return true
	}
	for _, se := range exprs {
		if ae, ok := se.(*sqlparser.AliasedExpr); ok {
			if fe, ok := ae.Expr.(*sqlparser.FuncExpr); ok && function.IsAggregate(fe.Name.Lowered()) {
				return true
			}
		}
	}
	return false
}

// aggregateSlots classifies each select item into an aggregate or a group-key
// passthrough, returning the per-slot value expression and the output columns.
func aggregateSlots(exprs sqlparser.SelectExprs) ([]runtime.Slot, []sqlparser.Expr, []*sqlparser.ColName, error) {
	var slots []runtime.Slot
	var slotExprs []sqlparser.Expr
	var columns []*sqlparser.ColName

	for _, se := range exprs {
		ae, ok := se.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, nil, nil, errors.Wrapf(ErrUnsupported, "aggregate select expr %T", se)
		}
		columns = append(columns, outputColumn(ae))

		if fe, ok := ae.Expr.(*sqlparser.FuncExpr); ok && function.IsAggregate(fe.Name.Lowered()) {
			arg, err := aggregateArg(fe)
			if err != nil {
				return nil, nil, nil, err
			}
			slots = append(slots, runtime.Slot{Aggregate: true, Name: fe.Name.Lowered()})
			slotExprs = append(slotExprs, arg)
			continue
		}
		slots = append(slots, runtime.Slot{})
		slotExprs = append(slotExprs, ae.Expr)
	}
	return slots, slotExprs, columns, nil
}

// aggregateArg returns the per-row argument expression for an aggregate call,
// mapping COUNT(*) and argument-less calls to the literal 1.
func aggregateArg(fe *sqlparser.FuncExpr) (sqlparser.Expr, error) {
	one := sqlparser.NewIntVal([]byte("1"))
	if len(fe.Exprs) == 0 {
		return one, nil
	}
	switch arg := fe.Exprs[0].(type) {
	case *sqlparser.StarExpr:
		return one, nil
	case *sqlparser.AliasedExpr:
		return arg.Expr, nil
	default:
		return nil, errors.Wrapf(ErrUnsupported, "aggregate argument %T", fe.Exprs[0])
	}
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
