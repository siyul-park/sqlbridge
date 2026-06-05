package compile

import (
	"context"
	"testing"

	"github.com/siyul-park/minivm/interp"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"

	"github.com/siyul-park/sqlbridge/catalog"
	"github.com/siyul-park/sqlbridge/runtime"
)

func benchCompiler(rows int) *Compiler {
	data := make([]catalog.Row, rows)
	for i := range data {
		data[i] = catalog.Row{
			Columns: []*sqlparser.ColName{col("a"), col("b")},
			Values:  []sqltypes.Value{sqltypes.NewInt64(int64(i % 50)), sqltypes.NewInt64(int64(i))},
		}
	}
	tbl := catalog.NewInMemoryTable(data)
	cat := catalog.NewInMemoryCatalog(map[string]catalog.Table{"t": tbl})
	return New(cat)
}

func runProgram(b *testing.B, prog *Program) {
	b.Helper()
	vm := interp.New(prog.Program)
	defer vm.Close()
	sess := runtime.NewSession(runtime.NewResult(prog.Columns))
	if err := vm.Run(runtime.WithSession(context.Background(), sess)); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkCompile(b *testing.B) {
	c := benchCompiler(100)
	stmt, err := sqlparser.Parse("select a, b from t where a > 10 order by b desc limit 5")
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Compile(stmt); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRun(b *testing.B) {
	c := benchCompiler(1000)
	stmt, _ := sqlparser.Parse("select a, b from t where a > 10 order by b desc limit 5")
	prog, err := c.Compile(stmt)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runProgram(b, prog)
	}
}

func BenchmarkAggregate(b *testing.B) {
	c := benchCompiler(1000)
	stmt, _ := sqlparser.Parse("select a, sum(b) from t group by a")
	prog, err := c.Compile(stmt)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runProgram(b, prog)
	}
}
