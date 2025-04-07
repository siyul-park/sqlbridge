package vm

import (
	"database/sql/driver"
	"testing"

	"github.com/siyul-park/sqlbridge/schema"

	"github.com/stretchr/testify/require"
	"github.com/xwb1989/sqlparser"
)

func TestVM_Eval(t *testing.T) {
	tests := []struct {
		record schema.Record
		expr   sqlparser.Expr
		value  driver.Value
	}{
		{
			expr: &sqlparser.ComparisonExpr{
				Operator: sqlparser.EqualStr,
				Left:     sqlparser.NewStrVal([]byte("foo")),
				Right:    sqlparser.NewStrVal([]byte("bar")),
			},
			value: false,
		},
		{
			expr: &sqlparser.ComparisonExpr{
				Operator: sqlparser.NotEqualStr,
				Left:     sqlparser.NewStrVal([]byte("foo")),
				Right:    sqlparser.NewStrVal([]byte("bar")),
			},
			value: true,
		},
		{
			expr:  sqlparser.NewStrVal([]byte("foo")),
			value: "foo",
		},
		{
			expr:  sqlparser.NewIntVal([]byte("1")),
			value: int64(1),
		},
		{
			expr:  sqlparser.NewFloatVal([]byte("0.25")),
			value: 0.25,
		},
		{
			expr:  sqlparser.NewHexNum([]byte("FF")),
			value: int64(0xff),
		},
		{
			expr:  sqlparser.NewHexVal([]byte("FF")),
			value: []byte{0xff},
		},
		{
			expr:  sqlparser.NewBitVal([]byte("10")),
			value: []byte{0x2},
		},
		{
			expr:  &sqlparser.NullVal{},
			value: nil,
		},
		{
			expr:  sqlparser.BoolVal(true),
			value: true,
		},
		{
			record: schema.Record{
				Columns: []*sqlparser.ColName{
					{Name: sqlparser.NewColIdent("id")},
					{Name: sqlparser.NewColIdent("name")},
				},
				Values: []driver.Value{1, "foo"},
			},
			expr:  &sqlparser.ColName{Name: sqlparser.NewColIdent("id")},
			value: 1,
		},
	}

	for _, tt := range tests {
		t.Run(sqlparser.String(tt.expr), func(t *testing.T) {
			vm := New(tt.record)
			val, err := vm.Eval(tt.expr)
			require.NoError(t, err)
			require.Equal(t, tt.value, val)
		})
	}
}
