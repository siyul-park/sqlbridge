# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

SQLBridge is a distributed SQL query engine exposed as a standard `database/sql` driver. It parses SQL with `xwb1989/sqlparser` and executes queries against pluggable backends (databases, NoSQL, REST APIs) that satisfy the `schema` interfaces. Go 1.24, CGO enabled.

## Commands

```bash
make test                          # go test -race ./...
make coverage                      # race + atomic coverage -> coverage.out
make benchmark                     # go test -bench=. -benchmem
make lint                          # goimports -w . + go vet ./...  (run before commit; CI gate)
make init                          # install goimports/godoc + go install ./...
make check                         # lint + test (full CI gate)

go test -race ./engine -run TestPlanner          # single package
go test -race ./engine -run TestPlanner/select   # single subtest
make benchmark test-options="-count=2"           # extra flags pass through test-options
```

CI runs `make init`, `make lint`, `make coverage` on Go 1.24; benchmarks compared via `benchstat` against base branch on `main`.

## Architecture

Three packages, strict dependency direction: `driver` → `engine` → `schema`.

### `schema/` — backend abstraction (the plugin boundary)

Nested lookup interfaces, each with `Composite*` (tries members in order, skips `ErrXxxNotFound`) and `InMemory*` implementations:

```
Registry.Catalog(name) → Catalog.Table(name) → Table.Scan(ctx, hint...) → Cursor → Row
```

To add a backend, implement `Registry`/`Catalog`/`Table`/`Cursor`. `Table` also exposes `Indexes()` and `Scan` accepts `ScanHint{Index, Ranges}` for **index push-down** (engine pushes range predicates into the source). `encoding.go` (`Marshal`/`Unmarshal`) bridges arbitrary Go/JSON values to `sqltypes.Value`.

### `engine/` — planning + execution

- **Planner** (`planner.go`) walks the sqlparser AST and builds a tree of `Plan` nodes. SELECT pipeline order: `From → Where → GroupBy → Having → SelectExprs → Distinct → OrderBy → Limit`. **Only SELECT is implemented**; INSERT/UPDATE/DELETE/DDL/etc. fall through to `driver.ErrSkip` (the cases exist but are empty).
- **Plan** (`plan.go`): iterator/volcano model — `Run(ctx, binds) → schema.Cursor`. One file per operator: `scan_plan`, `filter_plan`, `join_plan`, `group_plan`, `order_plan`, `limit_plan`, `distinct_plan`, `projection_plan`, `alias_plan`, `nop_plan`. Plans compose via `Walk`.
- **Expr** (`expr.go`): per-row expression tree — `Eval(ctx, row, binds) → Value`, plus `Walk`/`Copy`/`String`. One file per kind: `compare_expr`, `logical_expr`, `arithmetic_expr`, `call_expr`, `literal_expr`, `resolve_expr` (column refs), `subquery_expr`, `convert_expr`, `json_expr`, `time_expr`, `tuple_expr`, `inline_expr`, `transform_expr`.
- **Value** (`value.go`): typed wrapper over `querypb.Type` — `Int64`, `Uint64`, `Float64`, `VarChar`, `VarBinary`, `DateTime`, `Interval`, `JSON`, `Tuple`. `True`/`False` are `Int64` 1/0.
- **Dispatcher** (`dispatcher.go`): SQL function registry keyed by lowercased name. `WithFunction` registers; `WithBuiltIn` (`builtin_fn.go`) loads defaults. `NewBinaryFunction`/`NewTernaryFunction` adapt arity. `call_expr` resolves through it.

### `driver/` — `database/sql` adapter

`driver.New(WithRegistry(...), WithDispatcher(...))` builds a `Driver` (defaults: empty `InMemoryRegistry`, builtin dispatcher). `Open(name)` resolves the catalog named `name` and returns a `connection` holding a per-catalog `Planner`. `connection` implements the full set of `database/sql/driver` optional interfaces (ExecerContext, QueryerContext, ConnBeginTx, Pinger, Validator, SessionResetter). Files: `connection`, `connector`, `statement`, `rows`, `result`, `transaction`.

## Conventions

- Interfaces declare compile-time assertions: `var _ Iface = (*Impl)(nil)`.
- Errors wrapped with `github.com/pkg/errors` (`errors.WithStack`); sentinels checked via `errors.Is` (e.g. `ErrCatalogNotFound`, `ErrTableNotFound`).
- Every source file has a colocated `_test.go`; tests use `testify` and `go-faker` for fixtures. Keep coverage up — CI uploads to Codecov.
- Adding a Plan or Expr type = new `*_plan.go`/`*_expr.go` file implementing the interface + `Walk`/`String`, plus its `_test.go`.
