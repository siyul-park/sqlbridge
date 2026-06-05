package function

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/siyul-park/sqlbridge/value"
)

func ints(ns ...int64) []value.Value {
	vs := make([]value.Value, len(ns))
	for i, n := range ns {
		vs[i] = value.NewInt64(n)
	}
	return vs
}

func TestBitAnd(t *testing.T) {
	got, err := BitAnd()(ints(6, 3))
	require.NoError(t, err)
	assert.Equal(t, uint64(2), got.Interface())
}

func TestBitOr(t *testing.T) {
	got, err := BitOr()(ints(4, 1))
	require.NoError(t, err)
	assert.Equal(t, uint64(5), got.Interface())
}

func TestBitXor(t *testing.T) {
	got, err := BitXor()(ints(6, 3))
	require.NoError(t, err)
	assert.Equal(t, uint64(5), got.Interface())
}

func TestSubstr(t *testing.T) {
	tests := []struct {
		name string
		args []value.Value
		want string
	}{
		{"offset and length", []value.Value{value.NewVarChar("hello"), value.NewInt64(1), value.NewInt64(3)}, "ell"},
		{"empty", []value.Value{value.NewVarChar("")}, ""},
		{"no args", nil, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Substr()(tt.args)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got.Interface())
		})
	}
}

func TestConcatWs(t *testing.T) {
	got, err := ConcatWs()([]value.Value{value.NewVarChar("-"), value.NewVarChar("a"), value.NewVarChar("b")})
	require.NoError(t, err)
	assert.Equal(t, "a-b", got.Interface())
}

func TestNVL(t *testing.T) {
	got, err := NVL()([]value.Value{nil, value.NewInt64(9)})
	require.NoError(t, err)
	assert.Equal(t, int64(9), got.Interface())

	got, err = NVL()([]value.Value{value.NewInt64(1), value.NewInt64(9)})
	require.NoError(t, err)
	assert.Equal(t, int64(1), got.Interface())
}

func TestNVL2(t *testing.T) {
	got, err := NVL2()([]value.Value{value.NewInt64(1), value.NewVarChar("x"), value.NewVarChar("y")})
	require.NoError(t, err)
	assert.Equal(t, "x", got.Interface())

	got, err = NVL2()([]value.Value{nil, value.NewVarChar("x"), value.NewVarChar("y")})
	require.NoError(t, err)
	assert.Equal(t, "y", got.Interface())
}

func TestCount(t *testing.T) {
	got, err := Count()(ints(1, 2, 3))
	require.NoError(t, err)
	assert.Equal(t, int64(3), got.Interface())
}

func TestAvg(t *testing.T) {
	got, err := Avg()(ints(2, 4))
	require.NoError(t, err)
	assert.Equal(t, 3.0, got.Interface())

	got, err = Avg()(nil)
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestMax(t *testing.T) {
	got, err := Max()(ints(2, 9, 4))
	require.NoError(t, err)
	assert.Equal(t, int64(9), got.Interface())
}

func TestMin(t *testing.T) {
	got, err := Min()(ints(2, 9, 4))
	require.NoError(t, err)
	assert.Equal(t, int64(2), got.Interface())
}

func TestSum(t *testing.T) {
	got, err := Sum()(ints(1, 2, 3))
	require.NoError(t, err)
	assert.Equal(t, int64(6), got.Interface())
}

func TestStddevSamp(t *testing.T) {
	got, err := StddevSamp()(ints(2, 4, 4, 4, 5, 5, 7, 9))
	require.NoError(t, err)
	assert.InDelta(t, 2.138, got.Interface().(float64), 0.01)

	_, err = StddevSamp()(ints(1))
	assert.Error(t, err)
}

func TestStddevPop(t *testing.T) {
	got, err := StddevPop()(ints(2, 4, 4, 4, 5, 5, 7, 9))
	require.NoError(t, err)
	assert.InDelta(t, 2.0, got.Interface().(float64), 0.01)
}

func TestVarSamp(t *testing.T) {
	got, err := VarSamp()(ints(1, 2, 3))
	require.NoError(t, err)
	assert.Equal(t, 1.0, got.Interface())
}

func TestVarPop(t *testing.T) {
	got, err := VarPop()(ints(1, 2, 3))
	require.NoError(t, err)
	assert.InDelta(t, 0.6667, got.Interface().(float64), 0.01)
}

func TestWithBuiltin(t *testing.T) {
	d := New(WithBuiltin())
	for _, name := range []string{
		NameBitAnd, NameBitOr, NameBitXor, NameSubstr, NameConcatWs,
		NameNVL, NameNVL2, NameCount, NameAvg, NameMax, NameMin, NameSum,
		NameStd, NameStddev, NameStddevSamp, NameStddevPop, NameVariance,
		NameVarSamp, NameVarPop,
	} {
		_, ok := d.Lookup(name)
		assert.True(t, ok, "missing builtin %q", name)
	}

	got, err := d.Dispatch(NameSum, ints(10, 20))
	require.NoError(t, err)
	assert.Equal(t, int64(30), got.Interface())
}
