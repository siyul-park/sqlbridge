package runtime

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"

	"github.com/siyul-park/sqlbridge/catalog"
	"github.com/siyul-park/sqlbridge/function"
	"github.com/siyul-park/sqlbridge/value"
)

func TestArithmetic(t *testing.T) {
	tests := []struct {
		name string
		op   string
		lhs  value.Value
		rhs  value.Value
		want any
	}{
		{"int add", sqlparser.PlusStr, value.NewInt64(2), value.NewInt64(3), int64(5)},
		{"int sub", sqlparser.MinusStr, value.NewInt64(5), value.NewInt64(2), int64(3)},
		{"int mul", sqlparser.MultStr, value.NewInt64(4), value.NewInt64(3), int64(12)},
		{"int mod", sqlparser.ModStr, value.NewInt64(7), value.NewInt64(3), int64(1)},
		{"float div", sqlparser.DivStr, value.NewInt64(7), value.NewInt64(2), 3.5},
		{"float add promotes", sqlparser.PlusStr, value.NewInt64(1), value.NewFloat64(0.5), 1.5},
		{"bit and", sqlparser.BitAndStr, value.NewInt64(6), value.NewInt64(3), int64(2)},
		{"bit or", sqlparser.BitOrStr, value.NewInt64(4), value.NewInt64(1), int64(5)},
		{"bit xor", sqlparser.BitXorStr, value.NewInt64(6), value.NewInt64(3), int64(5)},
		{"shift left", sqlparser.ShiftLeftStr, value.NewInt64(1), value.NewInt64(3), int64(8)},
		{"shift right", sqlparser.ShiftRightStr, value.NewInt64(8), value.NewInt64(2), int64(2)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := arithmetic(tt.op, tt.lhs, tt.rhs)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got.Interface())
		})
	}

	t.Run("divide by zero yields NULL", func(t *testing.T) {
		got, err := arithmetic(sqlparser.DivStr, value.NewInt64(1), value.NewInt64(0))
		require.NoError(t, err)
		assert.Nil(t, got)
	})
}

func TestSatisfies(t *testing.T) {
	assert.True(t, satisfies(sqlparser.EqualStr, 0))
	assert.True(t, satisfies(sqlparser.NotEqualStr, 1))
	assert.True(t, satisfies(sqlparser.LessThanStr, -1))
	assert.True(t, satisfies(sqlparser.LessEqualStr, 0))
	assert.True(t, satisfies(sqlparser.GreaterThanStr, 1))
	assert.True(t, satisfies(sqlparser.GreaterEqualStr, 0))
	assert.False(t, satisfies(sqlparser.EqualStr, 1))
}

func TestGrouper(t *testing.T) {
	spec := &GrouperSpec{
		Columns:    []*sqlparser.ColName{{Name: sqlparser.NewColIdent("a")}, {Name: sqlparser.NewColIdent("s")}},
		Slots:      []Slot{{}, {Aggregate: true, Name: function.NameSum}},
		GroupCount: 1,
		Dispatcher: function.New(function.WithBuiltin()),
	}
	g := spec.New()
	// params: [identity(a), slot0(a passthrough), slot1(sum arg)]
	g.Row([]value.Value{value.NewInt64(1), value.NewInt64(1), value.NewInt64(10)})
	g.Row([]value.Value{value.NewInt64(1), value.NewInt64(1), value.NewInt64(20)})
	g.Row([]value.Value{value.NewInt64(2), value.NewInt64(2), value.NewInt64(30)})

	res := NewResult(spec.Columns)
	require.NoError(t, g.Finalize(res))

	rows := res.Rows()
	require.Len(t, rows, 2)
	assert.Equal(t, "1", rows[0].Values[0].ToString())
	assert.Equal(t, "30", rows[0].Values[1].ToString())
	assert.Equal(t, "2", rows[1].Values[0].ToString())
	assert.Equal(t, "30", rows[1].Values[1].ToString())
}

func TestGrouper_Cursor(t *testing.T) {
	spec := &GrouperSpec{
		Columns:    []*sqlparser.ColName{{Name: sqlparser.NewColIdent("c")}},
		Slots:      []Slot{{Aggregate: true, Name: function.NameCount}},
		GroupCount: 0,
		Dispatcher: function.New(function.WithBuiltin()),
	}
	g := spec.New()
	g.Row([]value.Value{value.NewInt64(1)})
	g.Row([]value.Value{value.NewInt64(1)})

	cur, err := g.Cursor()
	require.NoError(t, err)
	rows, err := catalog.ReadAll(cur)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "2", rows[0].Values[0].ToString())
}

func TestResult(t *testing.T) {
	col := &sqlparser.ColName{Name: sqlparser.NewColIdent("a")}

	t.Run("push and commit", func(t *testing.T) {
		r := NewResult([]*sqlparser.ColName{col})
		require.NoError(t, r.Push(value.NewInt64(1)))
		r.Commit()
		require.NoError(t, r.Push(value.NewInt64(2)))
		r.Commit()
		assert.Len(t, r.Rows(), 2)
	})

	t.Run("sort descending by key", func(t *testing.T) {
		r := NewResult([]*sqlparser.ColName{col})
		for _, n := range []int64{2, 1, 3} {
			require.NoError(t, r.Push(value.NewInt64(n)))
			r.PushKey(value.NewInt64(n))
			r.Commit()
		}
		r.Sort([]bool{true})
		assert.Equal(t, "3", r.Rows()[0].Values[0].ToString())
		assert.Equal(t, "1", r.Rows()[2].Values[0].ToString())
	})

	t.Run("distinct", func(t *testing.T) {
		r := NewResult([]*sqlparser.ColName{col})
		for _, n := range []int64{1, 1, 2} {
			require.NoError(t, r.Push(value.NewInt64(n)))
			r.Commit()
		}
		r.Distinct()
		assert.Len(t, r.Rows(), 2)
	})

	t.Run("limit and offset", func(t *testing.T) {
		r := NewResult([]*sqlparser.ColName{col})
		for _, n := range []int64{1, 2, 3, 4} {
			require.NoError(t, r.Push(value.NewInt64(n)))
			r.Commit()
		}
		r.Limit(1, 2)
		require.Len(t, r.Rows(), 2)
		assert.Equal(t, "2", r.Rows()[0].Values[0].ToString())
	})

	t.Run("emit row", func(t *testing.T) {
		r := NewResult(nil)
		r.EmitRow(catalog.Row{Columns: []*sqlparser.ColName{col}, Values: []sqltypes.Value{sqltypes.NewInt64(9)}})
		require.Len(t, r.Rows(), 1)
		assert.Equal(t, "9", r.Rows()[0].Values[0].ToString())
	})
}

func TestMergeRows(t *testing.T) {
	l := catalog.Row{
		Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("a")}},
		Values:  []sqltypes.Value{sqltypes.NewInt64(1)},
	}
	r := catalog.Row{
		Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("b")}},
		Values:  []sqltypes.Value{sqltypes.NewInt64(2)},
	}
	m := mergeRows(l, r)
	require.Len(t, m.Columns, 2)
	require.Len(t, m.Values, 2)
	assert.Equal(t, "1", m.Values[0].ToString())
	assert.Equal(t, "2", m.Values[1].ToString())
}
