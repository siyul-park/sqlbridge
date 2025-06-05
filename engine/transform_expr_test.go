package engine

import (
	"context"
	"testing"
	"time"

	"github.com/xwb1989/sqlparser/dependency/querypb"

	"github.com/siyul-park/sqlbridge/schema"
	"github.com/stretchr/testify/require"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
)

func TestDistinctExpr_Eval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	tests := []struct {
		input    Expr
		expected Value
	}{
		{
			input: &TupleExpr{
				Exprs: []Expr{
					&LiteralExpr{Value: sqltypes.NewInt64(1)},
					&LiteralExpr{Value: sqltypes.NewInt64(1)},
					&LiteralExpr{Value: sqltypes.NewInt64(1)},
				},
			},
			expected: NewTuple([]Value{NewInt64(1)}),
		},
		{
			input: &TupleExpr{
				Exprs: []Expr{
					&LiteralExpr{Value: sqltypes.NewInt64(1)},
					&LiteralExpr{Value: sqltypes.NewInt64(2)},
					&LiteralExpr{Value: sqltypes.NewInt64(3)},
				},
			},
			expected: NewTuple([]Value{
				NewInt64(1),
				NewInt64(2),
				NewInt64(3),
			}),
		},
		{
			input:    &TupleExpr{},
			expected: NewTuple(nil),
		},
	}

	for _, tt := range tests {
		expr := &DistinctExpr{Input: tt.input}
		t.Run(expr.String(), func(t *testing.T) {
			actual, err := expr.Eval(ctx, schema.Row{}, nil)
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual)
		})
	}
}

func TestOrderExpr_Eval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	tests := []struct {
		right     Expr
		direction string
		row       schema.Row
		expected  schema.Row
	}{
		{
			right:     &ColumnExpr{Value: &sqlparser.ColName{Name: sqlparser.NewColIdent("col1")}},
			direction: sqlparser.AscScr,
			row: schema.Row{
				Children: []schema.Row{
					{
						Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("col1")}},
						Values:  []sqltypes.Value{sqltypes.NewInt64(1)},
					},
					{
						Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("col1")}},
						Values:  []sqltypes.Value{sqltypes.NewInt64(2)},
					},
					{
						Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("col1")}},
						Values:  []sqltypes.Value{sqltypes.NewInt64(0)},
					},
				},
			},
			expected: schema.Row{
				Children: []schema.Row{
					{
						Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("col1")}},
						Values:  []sqltypes.Value{sqltypes.NewInt64(0)},
					},
					{
						Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("col1")}},
						Values:  []sqltypes.Value{sqltypes.NewInt64(1)},
					},
					{
						Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("col1")}},
						Values:  []sqltypes.Value{sqltypes.NewInt64(2)},
					},
				},
			},
		},
		{
			right:     &ColumnExpr{Value: &sqlparser.ColName{Name: sqlparser.NewColIdent("col1")}},
			direction: sqlparser.DescScr,
			row: schema.Row{
				Children: []schema.Row{
					{
						Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("col1")}},
						Values:  []sqltypes.Value{sqltypes.NewInt64(1)},
					},
					{
						Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("col1")}},
						Values:  []sqltypes.Value{sqltypes.NewInt64(2)},
					},
					{
						Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("col1")}},
						Values:  []sqltypes.Value{sqltypes.NewInt64(0)},
					},
				},
			},
			expected: schema.Row{
				Children: []schema.Row{
					{
						Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("col1")}},
						Values:  []sqltypes.Value{sqltypes.NewInt64(2)},
					},
					{
						Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("col1")}},
						Values:  []sqltypes.Value{sqltypes.NewInt64(1)},
					},
					{
						Columns: []*sqlparser.ColName{{Name: sqlparser.NewColIdent("col1")}},
						Values:  []sqltypes.Value{sqltypes.NewInt64(0)},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		var actual schema.Row
		expr := &OrderExpr{
			Left: Eval(func(ctx context.Context, row schema.Row, binds map[string]*querypb.BindVariable) (Value, error) {
				actual = row
				return nil, nil
			}),
			Right:     tt.right,
			Direction: tt.direction,
		}
		t.Run(expr.String(), func(t *testing.T) {
			_, err := expr.Eval(ctx, tt.row, nil)
			require.NoError(t, err)
			require.Equal(t, tt.expected, actual)
		})
	}
}
