package vm

import (
	"database/sql/driver"
	"testing"
	"time"

	"github.com/siyul-park/sqlbridge/schema"

	"github.com/stretchr/testify/require"
	"github.com/xwb1989/sqlparser"
)

func TestVM_Eval(t *testing.T) {
	vm := New()

	tests := []struct {
		record schema.Record
		args   []driver.NamedValue
		expr   sqlparser.Expr
		value  driver.Value
	}{
		{expr: sqlparser.BoolVal(true), value: true},
		{expr: sqlparser.BoolVal(false), value: false},

		{
			expr: &sqlparser.AndExpr{
				Left:  sqlparser.BoolVal(true),
				Right: sqlparser.BoolVal(false),
			},
			value: false,
		},
		{
			expr: &sqlparser.OrExpr{
				Left:  sqlparser.BoolVal(false),
				Right: sqlparser.BoolVal(true),
			},
			value: true,
		},
		{
			expr: &sqlparser.NotExpr{
				Expr: sqlparser.BoolVal(false),
			},
			value: true,
		},
		{
			expr: &sqlparser.ParenExpr{
				Expr: sqlparser.NewIntVal([]byte("42")),
			},
			value: int64(42),
		},
		{
			expr: &sqlparser.ComparisonExpr{
				Operator: sqlparser.EqualStr,
				Left:     sqlparser.NewStrVal([]byte("abc")),
				Right:    sqlparser.NewStrVal([]byte("abc")),
			},
			value: true,
		},
		{
			expr: &sqlparser.ComparisonExpr{
				Operator: sqlparser.NullSafeEqualStr,
				Left:     &sqlparser.NullVal{},
				Right:    &sqlparser.NullVal{},
			},
			value: true,
		},
		{
			expr: &sqlparser.ComparisonExpr{
				Operator: sqlparser.NotEqualStr,
				Left:     sqlparser.NewIntVal([]byte("1")),
				Right:    sqlparser.NewIntVal([]byte("2")),
			},
			value: true,
		},
		{
			expr: &sqlparser.ComparisonExpr{
				Operator: sqlparser.LessEqualStr,
				Left:     sqlparser.NewIntVal([]byte("1")),
				Right:    sqlparser.NewIntVal([]byte("1")),
			},
			value: true,
		},
		{
			expr: &sqlparser.ComparisonExpr{
				Operator: sqlparser.GreaterEqualStr,
				Left:     sqlparser.NewIntVal([]byte("2")),
				Right:    sqlparser.NewIntVal([]byte("2")),
			},
			value: true,
		},
		{
			expr: &sqlparser.ComparisonExpr{
				Operator: sqlparser.InStr,
				Left:     sqlparser.NewIntVal([]byte("3")),
				Right: sqlparser.ValTuple{
					sqlparser.NewIntVal([]byte("1")),
					sqlparser.NewIntVal([]byte("2")),
					sqlparser.NewIntVal([]byte("3")),
				},
			},
			value: true,
		},
		{
			expr: &sqlparser.ComparisonExpr{
				Operator: sqlparser.NotInStr,
				Left:     sqlparser.NewStrVal([]byte("x")),
				Right: sqlparser.ValTuple{
					sqlparser.NewStrVal([]byte("a")),
					sqlparser.NewStrVal([]byte("b")),
				},
			},
			value: true,
		},
		{
			expr: &sqlparser.ComparisonExpr{
				Operator: sqlparser.LikeStr,
				Left:     sqlparser.NewStrVal([]byte("abcde")),
				Right:    sqlparser.NewStrVal([]byte("abc%")),
			},
			value: true,
		},
		{
			expr: &sqlparser.ComparisonExpr{
				Operator: sqlparser.NotLikeStr,
				Left:     sqlparser.NewStrVal([]byte("abc")),
				Right:    sqlparser.NewStrVal([]byte("xyz%")),
			},
			value: true,
		},
		{
			expr: &sqlparser.ComparisonExpr{
				Operator: sqlparser.RegexpStr,
				Left:     sqlparser.NewStrVal([]byte("abc123")),
				Right:    sqlparser.NewStrVal([]byte("[0-9]+")),
			},
			value: true,
		},
		{
			expr: &sqlparser.ComparisonExpr{
				Operator: sqlparser.NotRegexpStr,
				Left:     sqlparser.NewStrVal([]byte("abc")),
				Right:    sqlparser.NewStrVal([]byte("[0-9]+")),
			},
			value: true,
		},
		{
			expr: &sqlparser.ComparisonExpr{
				Operator: sqlparser.JSONExtractOp,
				Left:     sqlparser.NewStrVal([]byte(`{"name": "chatgpt"}`)),
				Right:    sqlparser.NewStrVal([]byte("name")),
			},
			value: "chatgpt",
		},
		{
			expr: &sqlparser.ComparisonExpr{
				Operator: sqlparser.JSONUnquoteExtractOp,
				Left:     sqlparser.NewStrVal([]byte(`{"age": 30}`)),
				Right:    sqlparser.NewStrVal([]byte("age")),
			},
			value: "30",
		},
		{
			expr: &sqlparser.RangeCond{
				Left:     sqlparser.NewIntVal([]byte("5")),
				From:     sqlparser.NewIntVal([]byte("1")),
				To:       sqlparser.NewIntVal([]byte("10")),
				Operator: sqlparser.BetweenStr,
			},
			value: true,
		},
		{
			expr: &sqlparser.IsExpr{
				Expr:     sqlparser.BoolVal(true),
				Operator: sqlparser.IsTrueStr,
			},
			value: true,
		},
		{expr: sqlparser.NewStrVal([]byte("hello")), value: "hello"},
		{expr: sqlparser.NewIntVal([]byte("1000")), value: int64(1000)},
		{expr: sqlparser.NewFloatVal([]byte("3.1415")), value: 3.1415},
		{expr: sqlparser.NewHexNum([]byte("1A")), value: int64(0x1A)},
		{expr: sqlparser.NewHexVal([]byte("1F")), value: []byte{0x1F}},
		{expr: sqlparser.NewBitVal([]byte("1010")), value: []byte{0xA}},
		{
			expr:  sqlparser.NewValArg([]byte(":name")),
			args:  []driver.NamedValue{{Name: "name", Value: "Alice"}},
			value: "Alice",
		},
		{expr: &sqlparser.NullVal{}, value: nil},
		{expr: &sqlparser.Default{}, value: nil},
		{
			expr: &sqlparser.ColName{Name: sqlparser.NewColIdent("score")},
			record: schema.Record{
				Columns: []*sqlparser.ColName{
					{Name: sqlparser.NewColIdent("score")},
				},
				Values: []driver.Value{99.9},
			},
			value: 99.9,
		},
		{
			expr: sqlparser.ValTuple{
				sqlparser.NewIntVal([]byte("1")),
				sqlparser.NewStrVal([]byte("hello")),
				&sqlparser.NullVal{},
			},
			value: []driver.Value{int64(1), "hello", nil},
		},
		{
			expr: sqlparser.ListArg("::names"),
			args: []driver.NamedValue{
				{Name: "names", Value: []string{"a", "b"}},
			},
			value: []driver.Value{"a", "b"},
		},
		{
			expr: &sqlparser.BinaryExpr{
				Left:     sqlparser.NewIntVal([]byte("1")),
				Right:    sqlparser.NewIntVal([]byte("2")),
				Operator: sqlparser.PlusStr,
			},
			value: int64(3),
		},
		{
			expr: &sqlparser.BinaryExpr{
				Left:     sqlparser.NewStrVal([]byte("Hello ")),
				Right:    sqlparser.NewStrVal([]byte("World")),
				Operator: sqlparser.PlusStr,
			},
			value: "Hello World",
		},
		{
			expr: &sqlparser.BinaryExpr{
				Left:     sqlparser.NewIntVal([]byte("5")),
				Right:    sqlparser.NewIntVal([]byte("3")),
				Operator: sqlparser.MinusStr,
			},
			value: int64(2),
		},
		{
			expr: &sqlparser.BinaryExpr{
				Left:     sqlparser.NewIntVal([]byte("5")),
				Right:    sqlparser.NewIntVal([]byte("3")),
				Operator: sqlparser.MultStr,
			},
			value: int64(15),
		},
		{
			expr: &sqlparser.BinaryExpr{
				Left:     sqlparser.NewIntVal([]byte("10")),
				Right:    sqlparser.NewIntVal([]byte("2")),
				Operator: sqlparser.DivStr,
			},
			value: int64(5),
		},
		{
			expr: &sqlparser.BinaryExpr{
				Left:     sqlparser.NewIntVal([]byte("10")),
				Right:    sqlparser.NewIntVal([]byte("3")),
				Operator: sqlparser.IntDivStr,
			},
			value: int64(3),
		},
		{
			expr: &sqlparser.BinaryExpr{
				Left:     sqlparser.NewIntVal([]byte("10")),
				Right:    sqlparser.NewIntVal([]byte("3")),
				Operator: sqlparser.ModStr,
			},
			value: int64(1),
		},
		{
			expr: &sqlparser.BinaryExpr{
				Left:     sqlparser.NewIntVal([]byte("6")),
				Right:    sqlparser.NewIntVal([]byte("3")),
				Operator: sqlparser.BitAndStr,
			},
			value: int64(2),
		},
		{
			expr: &sqlparser.BinaryExpr{
				Left:     sqlparser.NewIntVal([]byte("6")),
				Right:    sqlparser.NewIntVal([]byte("3")),
				Operator: sqlparser.BitOrStr,
			},
			value: int64(7),
		},
		{
			expr: &sqlparser.BinaryExpr{
				Left:     sqlparser.NewIntVal([]byte("6")),
				Right:    sqlparser.NewIntVal([]byte("3")),
				Operator: sqlparser.BitXorStr,
			},
			value: int64(5),
		},
		{
			expr: &sqlparser.BinaryExpr{
				Left:     sqlparser.NewIntVal([]byte("2")),
				Right:    sqlparser.NewIntVal([]byte("3")),
				Operator: sqlparser.ShiftLeftStr,
			},
			value: int64(16),
		},
		{
			expr: &sqlparser.BinaryExpr{
				Left:     sqlparser.NewIntVal([]byte("16")),
				Right:    sqlparser.NewIntVal([]byte("3")),
				Operator: sqlparser.ShiftRightStr,
			},
			value: int64(2),
		},
		{
			expr: &sqlparser.UnaryExpr{
				Operator: sqlparser.UMinusStr,
				Expr:     sqlparser.NewIntVal([]byte("10")),
			},
			value: int64(-10),
		},
		{
			expr: &sqlparser.UnaryExpr{
				Operator: sqlparser.UPlusStr,
				Expr:     sqlparser.NewIntVal([]byte("10")),
			},
			value: int64(10),
		},
		{
			expr: &sqlparser.UnaryExpr{
				Operator: sqlparser.TildaStr,
				Expr:     sqlparser.NewIntVal([]byte("1")),
			},
			value: int64(^1),
		},
		{
			expr: &sqlparser.UnaryExpr{
				Operator: sqlparser.BangStr,
				Expr:     sqlparser.NewIntVal([]byte("0")),
			},
			value: true,
		},
		{
			expr: &sqlparser.UnaryExpr{
				Operator: sqlparser.BangStr,
				Expr:     sqlparser.NewIntVal([]byte("42")),
			},
			value: false,
		},
		{
			expr: &sqlparser.UnaryExpr{
				Operator: sqlparser.BinaryStr,
				Expr:     sqlparser.NewStrVal([]byte("abc")),
			},
			value: []byte("abc"),
		},
		{
			expr: &sqlparser.UnaryExpr{
				Operator: sqlparser.UBinaryStr,
				Expr:     sqlparser.NewStrVal([]byte("123")),
			},
			value: []byte("123"),
		},
		{
			expr: &sqlparser.IntervalExpr{
				Expr: sqlparser.NewIntVal([]byte("1")),
				Unit: "microsecond",
			},
			value: time.Microsecond,
		},
		{
			expr: &sqlparser.IntervalExpr{
				Expr: sqlparser.NewIntVal([]byte("2")),
				Unit: "second",
			},
			value: 2 * time.Second,
		},
		{
			expr: &sqlparser.IntervalExpr{
				Expr: sqlparser.NewIntVal([]byte("3")),
				Unit: "minute",
			},
			value: 3 * time.Minute,
		},
		{
			expr: &sqlparser.IntervalExpr{
				Expr: sqlparser.NewIntVal([]byte("4")),
				Unit: "hour",
			},
			value: 4 * time.Hour,
		},
		{
			expr: &sqlparser.IntervalExpr{
				Expr: sqlparser.NewIntVal([]byte("5")),
				Unit: "day",
			},
			value: 5 * 24 * time.Hour,
		},
		{
			expr: &sqlparser.IntervalExpr{
				Expr: sqlparser.NewIntVal([]byte("1")),
				Unit: "week",
			},
			value: 7 * 24 * time.Hour,
		},
		{
			expr: &sqlparser.IntervalExpr{
				Expr: sqlparser.NewIntVal([]byte("1")),
				Unit: "month",
			},
			value: (7 * 24 * 365 * time.Hour) / 12,
		},
		{
			expr: &sqlparser.IntervalExpr{
				Expr: sqlparser.NewIntVal([]byte("1")),
				Unit: "quarter",
			},
			value: (365 * 24 * time.Hour) / 4,
		},
		{
			expr: &sqlparser.IntervalExpr{
				Expr: sqlparser.NewIntVal([]byte("1")),
				Unit: "YEAR",
			},
			value: 365 * 24 * time.Hour,
		},
		{
			expr: &sqlparser.CollateExpr{
				Expr: sqlparser.NewStrVal([]byte("hello")),
			},
			value: "hello",
		},
		{
			expr: &sqlparser.FuncExpr{
				Name: sqlparser.NewColIdent("upper"),
				Exprs: sqlparser.SelectExprs{
					&sqlparser.AliasedExpr{Expr: sqlparser.NewStrVal([]byte("hello"))},
				},
			},
			value: "HELLO",
		},
		{
			expr: &sqlparser.FuncExpr{
				Name: sqlparser.NewColIdent("lower"),
				Exprs: sqlparser.SelectExprs{
					&sqlparser.AliasedExpr{Expr: sqlparser.NewStrVal([]byte("HELLO"))},
				},
			},
			value: "hello",
		},
		{
			expr: &sqlparser.FuncExpr{
				Name: sqlparser.NewColIdent("substr"),
				Exprs: sqlparser.SelectExprs{
					&sqlparser.AliasedExpr{Expr: sqlparser.NewStrVal([]byte("abcdef"))},
					&sqlparser.AliasedExpr{Expr: sqlparser.NewIntVal([]byte("1"))},
					&sqlparser.AliasedExpr{Expr: sqlparser.NewIntVal([]byte("3"))},
				},
			},
			value: "abc",
		},
		{
			expr: &sqlparser.FuncExpr{
				Name: sqlparser.NewColIdent("trim"),
				Exprs: sqlparser.SelectExprs{
					&sqlparser.AliasedExpr{Expr: sqlparser.NewStrVal([]byte("  hello  "))},
				},
			},
			value: "hello",
		},
		{
			expr: &sqlparser.FuncExpr{
				Name: sqlparser.NewColIdent("length"),
				Exprs: sqlparser.SelectExprs{
					&sqlparser.AliasedExpr{Expr: sqlparser.NewStrVal([]byte("abcd"))},
				},
			},
			value: 4,
		},
		{
			expr: &sqlparser.FuncExpr{
				Name: sqlparser.NewColIdent("concat"),
				Exprs: sqlparser.SelectExprs{
					&sqlparser.AliasedExpr{Expr: sqlparser.NewStrVal([]byte("foo"))},
					&sqlparser.AliasedExpr{Expr: sqlparser.NewStrVal([]byte("bar"))},
				},
			},
			value: "foobar",
		},
		{
			expr: &sqlparser.FuncExpr{
				Name: sqlparser.NewColIdent("abs"),
				Exprs: sqlparser.SelectExprs{
					&sqlparser.AliasedExpr{Expr: sqlparser.NewIntVal([]byte("-123"))},
				},
			},
			value: float64(123),
		},
		{
			expr: &sqlparser.FuncExpr{
				Name: sqlparser.NewColIdent("round"),
				Exprs: sqlparser.SelectExprs{
					&sqlparser.AliasedExpr{Expr: sqlparser.NewFloatVal([]byte("3.6"))},
				},
			},
			value: float64(4),
		},
		{
			expr: &sqlparser.FuncExpr{
				Name: sqlparser.NewColIdent("sqrt"),
				Exprs: sqlparser.SelectExprs{
					&sqlparser.AliasedExpr{Expr: sqlparser.NewIntVal([]byte("9"))},
				},
			},
			value: float64(3),
		},
		{
			expr: &sqlparser.FuncExpr{
				Name: sqlparser.NewColIdent("pow"),
				Exprs: sqlparser.SelectExprs{
					&sqlparser.AliasedExpr{Expr: sqlparser.NewIntVal([]byte("2"))},
					&sqlparser.AliasedExpr{Expr: sqlparser.NewIntVal([]byte("3"))},
				},
			},
			value: float64(8),
		},
		{
			expr: &sqlparser.FuncExpr{
				Name: sqlparser.NewColIdent("mod"),
				Exprs: sqlparser.SelectExprs{
					&sqlparser.AliasedExpr{Expr: sqlparser.NewIntVal([]byte("10"))},
					&sqlparser.AliasedExpr{Expr: sqlparser.NewIntVal([]byte("4"))},
				},
			},
			value: float64(2),
		},
		{
			expr: &sqlparser.FuncExpr{
				Name: sqlparser.NewColIdent("ifnull"),
				Exprs: sqlparser.SelectExprs{
					&sqlparser.AliasedExpr{Expr: &sqlparser.NullVal{}},
					&sqlparser.AliasedExpr{Expr: sqlparser.NewIntVal([]byte("123"))},
				},
			},
			value: int64(123),
		},
		{
			expr: &sqlparser.FuncExpr{
				Name: sqlparser.NewColIdent("nullif"),
				Exprs: sqlparser.SelectExprs{
					&sqlparser.AliasedExpr{Expr: sqlparser.NewIntVal([]byte("10"))},
					&sqlparser.AliasedExpr{Expr: sqlparser.NewIntVal([]byte("10"))},
				},
			},
			value: nil,
		},
		{
			expr: &sqlparser.FuncExpr{
				Name: sqlparser.NewColIdent("coalesce"),
				Exprs: sqlparser.SelectExprs{
					&sqlparser.AliasedExpr{Expr: &sqlparser.NullVal{}},
					&sqlparser.AliasedExpr{Expr: sqlparser.NewStrVal([]byte("foo"))},
					&sqlparser.AliasedExpr{Expr: sqlparser.NewStrVal([]byte("bar"))},
				},
			},
			value: "foo",
		},
		{
			expr: &sqlparser.FuncExpr{
				Name: sqlparser.NewColIdent("count"),
				Exprs: sqlparser.SelectExprs{
					&sqlparser.AliasedExpr{Expr: sqlparser.NewIntVal([]byte("1"))},
					&sqlparser.AliasedExpr{Expr: sqlparser.NewIntVal([]byte("2"))},
					&sqlparser.AliasedExpr{Expr: sqlparser.NewIntVal([]byte("3"))},
				},
			},
			value: 3,
		},
		{
			expr: &sqlparser.FuncExpr{
				Name: sqlparser.NewColIdent("sum"),
				Exprs: sqlparser.SelectExprs{
					&sqlparser.AliasedExpr{Expr: sqlparser.NewIntVal([]byte("1"))},
					&sqlparser.AliasedExpr{Expr: sqlparser.NewIntVal([]byte("2"))},
					&sqlparser.AliasedExpr{Expr: sqlparser.NewIntVal([]byte("3"))},
				},
			},
			value: float64(6),
		},
		{
			expr: &sqlparser.FuncExpr{
				Name: sqlparser.NewColIdent("avg"),
				Exprs: sqlparser.SelectExprs{
					&sqlparser.AliasedExpr{Expr: sqlparser.NewIntVal([]byte("2"))},
					&sqlparser.AliasedExpr{Expr: sqlparser.NewIntVal([]byte("4"))},
				},
			},
			value: float64(3),
		},
		{
			expr: &sqlparser.FuncExpr{
				Name: sqlparser.NewColIdent("min"),
				Exprs: sqlparser.SelectExprs{
					&sqlparser.AliasedExpr{Expr: sqlparser.NewIntVal([]byte("5"))},
					&sqlparser.AliasedExpr{Expr: sqlparser.NewIntVal([]byte("2"))},
					&sqlparser.AliasedExpr{Expr: sqlparser.NewIntVal([]byte("8"))},
				},
			},
			value: int64(2),
		},
		{
			expr: &sqlparser.FuncExpr{
				Name: sqlparser.NewColIdent("max"),
				Exprs: sqlparser.SelectExprs{
					&sqlparser.AliasedExpr{Expr: sqlparser.NewIntVal([]byte("5"))},
					&sqlparser.AliasedExpr{Expr: sqlparser.NewIntVal([]byte("2"))},
					&sqlparser.AliasedExpr{Expr: sqlparser.NewIntVal([]byte("8"))},
				},
			},
			value: int64(8),
		},
		{
			expr: &sqlparser.FuncExpr{
				Name: sqlparser.NewColIdent("group_concat"),
				Exprs: sqlparser.SelectExprs{
					&sqlparser.AliasedExpr{Expr: sqlparser.NewStrVal([]byte("a"))},
					&sqlparser.AliasedExpr{Expr: sqlparser.NewStrVal([]byte("b"))},
					&sqlparser.AliasedExpr{Expr: sqlparser.NewStrVal([]byte("c"))},
				},
			},
			value: "a,b,c",
		},
		{
			expr: &sqlparser.FuncExpr{
				Name: sqlparser.NewColIdent("bit_and"),
				Exprs: sqlparser.SelectExprs{
					&sqlparser.AliasedExpr{Expr: sqlparser.NewIntVal([]byte("7"))},
					&sqlparser.AliasedExpr{Expr: sqlparser.NewIntVal([]byte("3"))},
				},
			},
			value: int64(3),
		},
		{
			expr: &sqlparser.FuncExpr{
				Name: sqlparser.NewColIdent("bit_or"),
				Exprs: sqlparser.SelectExprs{
					&sqlparser.AliasedExpr{Expr: sqlparser.NewIntVal([]byte("4"))},
					&sqlparser.AliasedExpr{Expr: sqlparser.NewIntVal([]byte("2"))},
				},
			},
			value: int64(6),
		},
		{
			expr: &sqlparser.FuncExpr{
				Name: sqlparser.NewColIdent("bit_xor"),
				Exprs: sqlparser.SelectExprs{
					&sqlparser.AliasedExpr{Expr: sqlparser.NewIntVal([]byte("1"))},
					&sqlparser.AliasedExpr{Expr: sqlparser.NewIntVal([]byte("2"))},
					&sqlparser.AliasedExpr{Expr: sqlparser.NewIntVal([]byte("3"))},
				},
			},
			value: int64(0),
		},
		{
			expr: &sqlparser.CaseExpr{
				Expr: sqlparser.NewIntVal([]byte("2")),
				Whens: []*sqlparser.When{
					{
						Cond: sqlparser.NewIntVal([]byte("1")),
						Val:  sqlparser.NewStrVal([]byte("one")),
					},
					{
						Cond: sqlparser.NewIntVal([]byte("2")),
						Val:  sqlparser.NewStrVal([]byte("two")),
					},
				},
				Else: sqlparser.NewStrVal([]byte("other")),
			},
			value: "two",
		},
		{
			expr: &sqlparser.CaseExpr{
				Expr: sqlparser.NewIntVal([]byte("3")),
				Whens: []*sqlparser.When{
					{
						Cond: sqlparser.NewIntVal([]byte("1")),
						Val:  sqlparser.NewStrVal([]byte("one")),
					},
					{
						Cond: sqlparser.NewIntVal([]byte("2")),
						Val:  sqlparser.NewStrVal([]byte("two")),
					},
				},
				Else: sqlparser.NewStrVal([]byte("other")),
			},
			value: "other",
		},
		{
			expr: &sqlparser.CaseExpr{
				Whens: []*sqlparser.When{
					{
						Cond: sqlparser.BoolVal(true),
						Val:  sqlparser.NewStrVal([]byte("matched")),
					},
					{
						Cond: sqlparser.BoolVal(false),
						Val:  sqlparser.NewStrVal([]byte("unmatched")),
					},
				},
			},
			value: "matched",
		},
		{
			expr: &sqlparser.CaseExpr{
				Expr: sqlparser.NewIntVal([]byte("10")),
				Whens: []*sqlparser.When{
					{
						Cond: sqlparser.NewIntVal([]byte("1")),
						Val:  sqlparser.NewStrVal([]byte("one")),
					},
				},
			},
			value: nil,
		},
		{
			expr: &sqlparser.ValuesFuncExpr{
				Name: &sqlparser.ColName{Name: sqlparser.NewColIdent("username")},
			},
			record: schema.Record{
				Columns: []*sqlparser.ColName{
					{Name: sqlparser.NewColIdent("username")},
				},
				Values: []driver.Value{"alice"},
			},
			value: "alice",
		},
		{
			expr: &sqlparser.ConvertExpr{
				Expr: sqlparser.NewStrVal([]byte("true")),
				Type: &sqlparser.ConvertType{Type: "bool"},
			},
			value: true,
		},
		{
			expr: &sqlparser.ConvertExpr{
				Expr: sqlparser.NewStrVal([]byte("123")),
				Type: &sqlparser.ConvertType{Type: "integer"},
			},
			value: int64(123),
		},
		{
			expr: &sqlparser.ConvertExpr{
				Expr: sqlparser.NewStrVal([]byte("3.14")),
				Type: &sqlparser.ConvertType{Type: "float"},
			},
			value: float64(3.14),
		},
		{
			expr: &sqlparser.ConvertExpr{
				Expr: sqlparser.NewIntVal([]byte("42")),
				Type: &sqlparser.ConvertType{Type: "varchar"},
			},
			value: "42",
		},
		{
			expr: &sqlparser.ConvertExpr{
				Expr: sqlparser.NewStrVal([]byte("hello")),
				Type: &sqlparser.ConvertType{Type: "binary"},
			},
			value: []byte("hello"),
		},
		{
			expr: &sqlparser.ConvertExpr{
				Expr: sqlparser.NewStrVal([]byte("2024-01-01")),
				Type: &sqlparser.ConvertType{Type: "date"},
			},
			value: func() driver.Value {
				t, _ := time.Parse("2006-01-02", "2024-01-01")
				return t.UTC().Nanosecond()
			}(),
		},
		{
			expr: &sqlparser.SubstrExpr{
				Name: &sqlparser.ColName{Name: sqlparser.NewColIdent("text")},
				From: sqlparser.NewIntVal([]byte("7")),
				To:   sqlparser.NewIntVal([]byte("5")),
			},
			record: schema.Record{
				Columns: []*sqlparser.ColName{
					{Name: sqlparser.NewColIdent("text")},
				},
				Values: []driver.Value{"hello world"},
			},
			value: "world",
		},
		{
			expr: &sqlparser.SubstrExpr{
				Name: &sqlparser.ColName{Name: sqlparser.NewColIdent("str")},
				From: sqlparser.NewIntVal([]byte("2")),
			},
			record: schema.Record{
				Columns: []*sqlparser.ColName{
					{Name: sqlparser.NewColIdent("str")},
				},
				Values: []driver.Value{"abcdef"},
			},
			value: "bcdef",
		},
		{
			expr: &sqlparser.SubstrExpr{
				Name: &sqlparser.ColName{Name: sqlparser.NewColIdent("data")},
				From: sqlparser.NewIntVal([]byte("1")),
				To:   sqlparser.NewIntVal([]byte("3")),
			},
			record: schema.Record{
				Columns: []*sqlparser.ColName{
					{Name: sqlparser.NewColIdent("data")},
				},
				Values: []driver.Value{"abcdef"},
			},
			value: "abc",
		},
		{
			expr: &sqlparser.MatchExpr{
				Columns: sqlparser.SelectExprs{
					&sqlparser.AliasedExpr{Expr: &sqlparser.ColName{Name: sqlparser.NewColIdent("c1")}},
					&sqlparser.StarExpr{},
				},
				Expr:   sqlparser.NewStrVal([]byte("x")),
				Option: "",
			},
			record: schema.Record{
				Columns: []*sqlparser.ColName{
					{Name: sqlparser.NewColIdent("c1")},
					{Name: sqlparser.NewColIdent("c2")},
				},
				Values: []driver.Value{"x foo", "bar x baz"},
			},
			value: float64(3),
		},
		{
			expr: &sqlparser.GroupConcatExpr{
				Exprs: sqlparser.SelectExprs{&sqlparser.AliasedExpr{Expr: &sqlparser.ColName{Name: sqlparser.NewColIdent("c1")}}},
			},
			record: schema.Record{
				Columns: []*sqlparser.ColName{schema.GroupColumn},
				Values: []driver.Value{[]schema.Record{
					{Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("c1")}}, Values: []driver.Value{"a"}},
					{Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("c1")}}, Values: []driver.Value{"b"}},
					{Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("c1")}}, Values: []driver.Value{"a"}},
				}},
			},
			value: "a,b,a",
		},
		{
			expr: &sqlparser.GroupConcatExpr{
				Distinct:  "distinct",
				Exprs:     sqlparser.SelectExprs{&sqlparser.AliasedExpr{Expr: &sqlparser.ColName{Name: sqlparser.NewColIdent("c1")}}},
				Separator: ";",
			},
			record: schema.Record{
				Columns: []*sqlparser.ColName{schema.GroupColumn},
				Values: []driver.Value{[]schema.Record{
					{Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("c1")}}, Values: []driver.Value{"x"}},
					{Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("c1")}}, Values: []driver.Value{"y"}},
					{Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("c1")}}, Values: []driver.Value{"x"}},
				}},
			},
			value: "x;y",
		},
		{
			expr: &sqlparser.GroupConcatExpr{
				Exprs: sqlparser.SelectExprs{
					&sqlparser.AliasedExpr{Expr: &sqlparser.ColName{Name: sqlparser.NewColIdent("c1")}},
					&sqlparser.AliasedExpr{Expr: &sqlparser.ColName{Name: sqlparser.NewColIdent("c2")}},
				},
				Separator: "|",
			},
			record: schema.Record{
				Columns: []*sqlparser.ColName{schema.GroupColumn},
				Values: []driver.Value{[]schema.Record{
					{Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("c1")}, {Name: sqlparser.NewColIdent("c2")}}, Values: []driver.Value{"a", "1"}},
					{Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("c1")}, {Name: sqlparser.NewColIdent("c2")}}, Values: []driver.Value{"b", "2"}},
				}},
			},
			value: "a1|b2",
		},
		{
			expr: &sqlparser.GroupConcatExpr{
				Exprs:   sqlparser.SelectExprs{&sqlparser.AliasedExpr{Expr: &sqlparser.ColName{Name: sqlparser.NewColIdent("c1")}}},
				OrderBy: sqlparser.OrderBy{{Expr: &sqlparser.ColName{Name: sqlparser.NewColIdent("c1")}, Direction: sqlparser.AscScr}},
			},
			record: schema.Record{
				Columns: []*sqlparser.ColName{schema.GroupColumn},
				Values: []driver.Value{[]schema.Record{
					{Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("c1")}}, Values: []driver.Value{"b"}},
					{Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("c1")}}, Values: []driver.Value{"a"}},
				}},
			},
			value: "a,b",
		},
		{
			expr:  &sqlparser.Default{},
			value: nil,
		},
	}

	for _, tt := range tests {
		t.Run(sqlparser.String(tt.expr), func(t *testing.T) {
			val, err := vm.Eval(tt.expr, tt.record, tt.args...)
			require.NoError(t, err)
			require.Equal(t, tt.value, val)
		})
	}
}
