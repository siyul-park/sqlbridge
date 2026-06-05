# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

SQLBridge is a SQL query engine exposed as a standard `database/sql` driver. It parses SQL with `xwb1989/sqlparser` (which carries Vitess `sqltypes`/`querypb`) and **compiles each statement into a single bytecode program for the minivm VM** (`github.com/siyul-park/minivm`), which runs it against pluggable backends that satisfy the `catalog` interfaces. Go 1.26.2, CGO enabled.

The execution model is "full query → bytecode": the scan loop, WHERE filter, projection, GROUP BY, ORDER BY, LIMIT, and writes are emitted as minivm bytecode. Host functions bridge storage I/O and SQL value semantics; the bytecode owns control and data flow.

## Commands

```bash
make test                          # go test -race ./...
make coverage                      # race + atomic coverage -> coverage.out
make benchmark                     # go test -bench=. -benchmem
make lint                          # goimports -w . + go vet ./...  (run before commit; CI gate)
make check                         # lint + test (full CI gate)

go test -race ./compile -run TestCompiler            # single package
go test -race ./compile -run TestCompiler_Compile/order_by_descending  # single subtest
```

CI runs `make init`, `make lint`, `make coverage` on Go 1.26.

## Architecture

Dependency direction: `sqldriver → compile → {runtime, function, value} → catalog`. `runtime` wraps minivm.

### `catalog/` — backend abstraction (the plugin boundary)

Nested lookup interfaces, each with `Composite*` (tries members in order, skips `ErrXxxNotFound`) and `InMemory*` implementations:

```
Registry.Catalog(name) → Catalog.Table(name) → Table.Scan(ctx, hint...) → Cursor → Row
```

`Table` exposes `Indexes()` and `Scan` accepts `ScanHint{Index, Ranges}` for index push-down. Mutable backends also implement `Writer` (`Insert`/`Delete`); `InMemoryTable` does. `encoding.go` (`Marshal`/`Unmarshal`) bridges Go/JSON values to `sqltypes.Value`.

### `value/` — SQL value model

Typed wrapper over `querypb.Type` — `Int64`, `Uint64`, `Float64`, `VarChar`, `VarBinary`, `DateTime`, `Interval`, `JSON`, `Tuple`. `True`/`False` are `Int64` 1/0. Provides `Compare`/`Promote`/`Cast`/`FromSQL`/`ToSQL` and `To*` conversions. NULL is a nil `Value`.

### `runtime/` — the minivm bridge

- **boxing** (`value.go`): SQL `value.Value` ⇄ minivm `types.Value`. Common types map to **native** minivm carriers — Int64→`I64`, Float64→`F64`, VarChar→`String`, VarBinary→`TypedArray[int8]`, Tuple→`*Array` — so the VM/JIT sees real values. A heap `cell` wraps only types without a faithful native carrier (Uint64/DateTime/Interval/JSON) since minivm `Boxed` carries no SQL type tag. `Box`/`Unbox` move values to/from the stack; `Constant` embeds literals in a program's constant table.
- **host functions** (`sqlhost.go`, `arith.go`, `dispatch.go`, `group.go`, `write.go`): the operations a compiled program calls — `OpenFunc`/`NextFunc` (scan), `ColumnFunc`, `CompareFunc`/`AndFunc`/`OrFunc`/`NotFunc`/`ArithmeticFunc`, `TruthyFunc`, `PushFunc`/`CommitFunc`/`EmitStarFunc`/`PushKeyFunc`/`SortFunc`/`LimitFunc`, `DispatchFunc` (scalar functions), the `Grouper` host fns (GROUP BY/aggregates), and `InsertFunc`/`DeleteCurrentFunc`/`UpdateCurrentFunc`.
- **Session** (`sqlhost.go`): per-execution state (cursor, current row, `Result` accumulator, `Grouper`, affected count) passed through the run context with `WithSession`; host functions read it via `vm.Context()`. No VM globals or heap are used for it, so it survives reference counting.
- **HostFunc** (`host.go`): adapts a Go `func([]value.Value) (value.Value, error)` into a minivm host function via the boxing layer.

### `function/` — SQL function dispatcher

Case-insensitive registry keyed by lowercased name. `WithFunction` registers; `WithBuiltin` loads scalar + aggregate defaults (`builtin.go`). `NewBinaryFunction`/`NewTernaryFunction` adapt arity. `IsAggregate` tells the compiler whether a name is an aggregate (needs GROUP BY) or a scalar.

### `compile/` — SQL → minivm bytecode (the engine)

- **Builder** (`builder.go`): assembles `[]instr.Instruction` with label-based branches; emits placeholder branch operands and patches them to signed byte-relative offsets in `Build`.
- **Compiler** (`compiler.go`): `New(catalog, opts...)` → `Compile(stmt) → *Program`. SELECT emits scan loop + WHERE + projection (or `EmitStar`) + ORDER BY keys + sort + LIMIT. Aggregate/GROUP BY queries take the `compileAggregate` path feeding a host `Grouper`. Expressions compile in `expr.go` (columns, literals, comparisons, logic, arithmetic, scalar functions). Writes compile in `write.go` (INSERT unrolled per tuple; DELETE/UPDATE over a scan loop via `scanLoop`). `WithOptimizer` enables minivm's optimizer (off by default — its passes do not preserve the branch offsets these programs rely on).

### `sqldriver/` — `database/sql` adapter

`sqldriver.New(WithRegistry(...))` builds a `Driver`. `Open(name)`/`OpenConnector(name)` resolve the catalog and return a `connection` holding a per-catalog `compile.Compiler`. `connection` implements `QueryerContext` (compile + run → `rows`) and `ExecerContext` (compile + run → rows-affected). Each execution builds a fresh `interp.Interpreter`, seeds a `runtime.Session` through the context, runs the program, and reads results from the session.

## Conventions

- Interfaces declare compile-time assertions: `var _ Iface = (*Impl)(nil)`.
- Errors wrapped with `github.com/pkg/errors` (`errors.WithStack`/`Wrap`); sentinels checked via `errors.Is` (e.g. `ErrCatalogNotFound`, `compile.ErrUnsupported`).
- Every source file has a colocated `_test.go`; tests use `testify`. Run with `-race`.
- Adding an operator/expression = a new host function in `runtime/` plus its emission in `compile/`. Unsupported constructs return `compile.ErrUnsupported`.

## Not yet implemented

JOIN, DISTINCT, HAVING, subqueries, bind variables/prepared parameters, and transactions return `compile.ErrUnsupported` or are absent. The minivm optimizer is incompatible with the emitted branch offsets (kept opt-in).
