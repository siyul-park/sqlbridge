package compile

import (
	"github.com/pkg/errors"
	"github.com/siyul-park/minivm/instr"
	"github.com/siyul-park/minivm/program"
	"github.com/xwb1989/sqlparser"

	"github.com/siyul-park/sqlbridge/catalog"
	"github.com/siyul-park/sqlbridge/runtime"
)

// compileInsert compiles INSERT ... VALUES into a program that builds and
// inserts each row tuple.
func (c *Compiler) compileInsert(ins *sqlparser.Insert) (*Program, error) {
	tbl, err := c.catalog.Table(ins.Table.Name.String())
	if err != nil {
		return nil, err
	}
	w, err := writerOf(tbl)
	if err != nil {
		return nil, err
	}

	values, ok := ins.Rows.(sqlparser.Values)
	if !ok {
		return nil, errors.Wrapf(ErrUnsupported, "insert rows %T", ins.Rows)
	}

	columns := make([]*sqlparser.ColName, len(ins.Columns))
	for i, ci := range ins.Columns {
		columns[i] = &sqlparser.ColName{Name: ci}
	}

	g := &gen{cat: c.catalog, dispatcher: c.dispatcher, b: NewBuilder()}
	insert := g.host(runtime.InsertFunc(w, columns))

	for _, tuple := range values {
		if len(tuple) != len(columns) {
			return nil, errors.Wrap(ErrUnsupported, "insert column/value count mismatch")
		}
		for _, expr := range tuple {
			if err := compileExpr(g, expr); err != nil {
				return nil, err
			}
		}
		g.b.Emit(instr.CONST_GET, insert).Emit(instr.CALL).Emit(instr.DROP)
	}

	return c.program(g)
}

// compileDelete compiles DELETE ... WHERE, removing each matching scanned row.
func (c *Compiler) compileDelete(del *sqlparser.Delete) (*Program, error) {
	tbl, err := resolveTable(c.catalog, del.TableExprs)
	if err != nil {
		return nil, err
	}
	w, err := writerOf(tbl)
	if err != nil {
		return nil, err
	}

	g := &gen{cat: c.catalog, dispatcher: c.dispatcher, b: NewBuilder()}
	remove := g.host(runtime.DeleteCurrentFunc(w))

	if err := scanLoop(g, tbl, del.Where, func() error {
		g.b.Emit(instr.CONST_GET, remove).Emit(instr.CALL).Emit(instr.DROP)
		return nil
	}); err != nil {
		return nil, err
	}

	return c.program(g)
}

// compileUpdate compiles UPDATE ... SET ... WHERE, rewriting each matching row.
func (c *Compiler) compileUpdate(upd *sqlparser.Update) (*Program, error) {
	tbl, err := resolveTable(c.catalog, upd.TableExprs)
	if err != nil {
		return nil, err
	}
	w, err := writerOf(tbl)
	if err != nil {
		return nil, err
	}

	columns := make([]*sqlparser.ColName, len(upd.Exprs))
	exprs := make([]sqlparser.Expr, len(upd.Exprs))
	for i, ue := range upd.Exprs {
		columns[i] = ue.Name
		exprs[i] = ue.Expr
	}

	g := &gen{cat: c.catalog, dispatcher: c.dispatcher, b: NewBuilder()}
	update := g.host(runtime.UpdateCurrentFunc(w, columns))

	if err := scanLoop(g, tbl, upd.Where, func() error {
		for _, expr := range exprs {
			if err := compileExpr(g, expr); err != nil {
				return err
			}
		}
		g.b.Emit(instr.CONST_GET, update).Emit(instr.CALL).Emit(instr.DROP)
		return nil
	}); err != nil {
		return nil, err
	}

	return c.program(g)
}

// scanLoop emits the open/next/where scan skeleton, invoking body for each row
// that passes the predicate.
func scanLoop(g *gen, tbl catalog.Table, where *sqlparser.Where, body func() error) error {
	open := g.host(runtime.OpenFunc(tbl))
	next := g.host(runtime.NextFunc())

	g.b.Emit(instr.CONST_GET, open).Emit(instr.CALL).Emit(instr.DROP)

	loop := g.b.Label()
	done := g.b.Label()

	g.b.Bind(loop)
	g.b.Emit(instr.CONST_GET, next).Emit(instr.CALL).Emit(instr.I32_EQZ)
	g.b.Branch(instr.BR_IF, done)

	if where != nil {
		if err := compileExpr(g, where.Expr); err != nil {
			return err
		}
		g.b.Emit(instr.CONST_GET, g.host(runtime.TruthyFunc())).Emit(instr.CALL).Emit(instr.I32_EQZ)
		g.b.Branch(instr.BR_IF, loop)
	}

	if err := body(); err != nil {
		return err
	}

	g.b.Branch(instr.BR, loop)
	g.b.Bind(done)
	return nil
}

// program finalizes the builder into a write Program.
func (c *Compiler) program(g *gen) (*Program, error) {
	insts, err := g.b.Build()
	if err != nil {
		return nil, err
	}
	return &Program{
		Program: program.New(insts, program.WithConstants(g.consts...)),
		Globals: runtime.GlobalCount,
	}, nil
}

// writerOf asserts that tbl supports mutation.
func writerOf(tbl catalog.Table) (catalog.Writer, error) {
	w, ok := tbl.(catalog.Writer)
	if !ok {
		return nil, errors.Wrap(ErrUnsupported, "table is read-only")
	}
	return w, nil
}
