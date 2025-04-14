package eval

import (
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"math/big"
	"strconv"

	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/querypb"
	"github.com/xwb1989/sqlparser/dependency/sqltypes"
)

type Builder struct {
	dispatcher *Dispatcher
}

func NewBuilder(opts ...Option) *Builder {
	if len(opts) == 0 {
		opts = append(opts, Builtin())
	}
	return &Builder{dispatcher: NewDispatcher(opts...)}
}

func (b *Builder) Build(expr sqlparser.Expr) (Expr, error) {
	switch expr := expr.(type) {
	case *sqlparser.AndExpr:
		return b.buildAndExpr(expr)
	case *sqlparser.OrExpr:
		return b.buildOrExpr(expr)
	case *sqlparser.NotExpr:
		return b.buildNotExpr(expr)
	case *sqlparser.ParenExpr:
		return b.buildParenExpr(expr)
	case *sqlparser.ComparisonExpr:
		return b.buildComparisonExpr(expr)
	case *sqlparser.RangeCond:
		return b.buildRangeCond(expr)
	case *sqlparser.IsExpr:
		return b.buildIsExpr(expr)
	case *sqlparser.ExistsExpr:
		return b.buildExistsExpr(expr)
	case *sqlparser.SQLVal:
		return b.buildSQLVal(expr)
	case *sqlparser.NullVal:
		return b.buildNullValue(expr)
	case sqlparser.BoolVal:
		return b.buildBoolVal(expr)
	case *sqlparser.ColName:
		return b.buildColName(expr)
	case sqlparser.ValTuple:
		return b.buildValTuple(expr)
	case sqlparser.ListArg:
		return b.buildListArg(expr)
	case *sqlparser.BinaryExpr:
		return b.buildBinaryExpr(expr)
	case *sqlparser.UnaryExpr:
		return b.buildUnaryExpr(expr)
	case *sqlparser.IntervalExpr:
		return b.buildIntervalExpr(expr)
	case *sqlparser.CollateExpr:
	case *sqlparser.FuncExpr:
		return b.buildFuncExpr(expr)
	case *sqlparser.CaseExpr:
		return b.buildCaseExpr(expr)
	case *sqlparser.ValuesFuncExpr:
		return b.buildValuesFuncExpr(expr)
	case *sqlparser.ConvertExpr:
		return b.buildConvertExpr(expr)
	case *sqlparser.SubstrExpr:
		return b.buildSubstrExpr(expr)
	case *sqlparser.ConvertUsingExpr:
	case *sqlparser.MatchExpr:
		return b.buildMatchExpr(expr)
	case *sqlparser.GroupConcatExpr:
	case *sqlparser.Default:
	}
	return nil, driver.ErrSkip
}

func (b *Builder) buildAndExpr(expr *sqlparser.AndExpr) (Expr, error) {
	left, err := b.Build(expr.Left)
	if err != nil {
		return nil, err
	}
	right, err := b.Build(expr.Right)
	if err != nil {
		return nil, err
	}
	return &And{Left: left, Right: right}, nil
}

func (b *Builder) buildOrExpr(expr *sqlparser.OrExpr) (Expr, error) {
	left, err := b.Build(expr.Left)
	if err != nil {
		return nil, err
	}
	right, err := b.Build(expr.Right)
	if err != nil {
		return nil, err
	}
	return &Or{Left: left, Right: right}, nil
}

func (b *Builder) buildNotExpr(expr *sqlparser.NotExpr) (Expr, error) {
	input, err := b.Build(expr.Expr)
	if err != nil {
		return nil, err
	}
	return &Not{Input: input}, nil
}

func (b *Builder) buildParenExpr(expr *sqlparser.ParenExpr) (Expr, error) {
	return b.Build(expr.Expr)
}

func (b *Builder) buildComparisonExpr(expr *sqlparser.ComparisonExpr) (Expr, error) {
	left, err := b.Build(expr.Left)
	if err != nil {
		return nil, err
	}
	right, err := b.Build(expr.Right)
	if err != nil {
		return nil, err
	}

	switch expr.Operator {
	case sqlparser.EqualStr, sqlparser.NullSafeEqualStr:
		return &Equal{Left: left, Right: right}, nil
	case sqlparser.NotEqualStr:
		return &Not{Input: &Equal{Left: left, Right: right}}, nil
	case sqlparser.LessThanStr:
		return &LessThan{Left: left, Right: right}, nil
	case sqlparser.LessEqualStr:
		return &LessThanEqual{Left: left, Right: right}, nil
	case sqlparser.GreaterThanStr:
		return &GreaterThan{Left: left, Right: right}, nil
	case sqlparser.GreaterEqualStr:
		return &GreaterThanEqual{Left: left, Right: right}, nil
	case sqlparser.InStr:
		return &In{Left: left, Right: right}, nil
	case sqlparser.NotInStr:
		return &Not{Input: &In{Left: left, Right: right}}, nil
	case sqlparser.LikeStr:
		return &Like{Left: left, Right: right}, nil
	case sqlparser.NotLikeStr:
		return &Not{Input: &Like{Left: left, Right: right}}, nil
	case sqlparser.RegexpStr:
		return &Regexp{Left: left, Right: right}, nil
	case sqlparser.NotRegexpStr:
		return &Not{Input: &Regexp{Left: left, Right: right}}, nil
	case sqlparser.JSONExtractOp:
		return &JSONExtract{Left: left, Right: right}, nil
	case sqlparser.JSONUnquoteExtractOp:
		return &Convert{Input: &JSONExtract{Left: left, Right: right}, Type: &sqlparser.ConvertType{Type: querypb.Type_name[int32(querypb.Type_VARCHAR)]}}, nil
	default:
		return nil, driver.ErrSkip
	}
}

func (b *Builder) buildRangeCond(expr *sqlparser.RangeCond) (Expr, error) {
	left, err := b.Build(expr.Left)
	if err != nil {
		return nil, err
	}
	from, err := b.Build(expr.From)
	if err != nil {
		return nil, err
	}
	to, err := b.Build(expr.To)
	if err != nil {
		return nil, err
	}

	input := Expr(&And{
		Left: &GreaterThanEqual{
			Left:  left,
			Right: from,
		},
		Right: &LessThanEqual{
			Left:  left,
			Right: to,
		},
	})
	if expr.Operator == sqlparser.NotBetweenStr {
		input = &Not{Input: input}
	}
	return input, nil
}

func (b *Builder) buildIsExpr(expr *sqlparser.IsExpr) (Expr, error) {
	input, err := b.Build(expr.Expr)
	if err != nil {
		return nil, err
	}

	switch expr.Operator {
	case sqlparser.IsNullStr:
		return &IsNull{Input: input}, nil
	case sqlparser.IsNotNullStr:
		return &Not{Input: &IsNull{Input: input}}, nil
	case sqlparser.IsTrueStr:
		return &IsTrue{Input: input}, nil
	case sqlparser.IsFalseStr:
		return &Not{Input: &IsTrue{Input: input}}, nil
	default:
		return nil, driver.ErrSkip
	}
}

func (b *Builder) buildExistsExpr(expr *sqlparser.ExistsExpr) (Expr, error) {
	input, err := b.Build(expr.Subquery)
	if err != nil {
		return nil, err
	}
	return &Exists{Input: input}, nil
}

func (b *Builder) buildSQLVal(expr *sqlparser.SQLVal) (Expr, error) {
	switch expr.Type {
	case sqlparser.StrVal:
		val, err := sqltypes.NewValue(sqltypes.VarChar, expr.Val)
		if err != nil {
			return nil, err
		}
		return &Literal{Value: val}, nil
	case sqlparser.IntVal:
		val, err := sqltypes.NewValue(sqltypes.Int64, expr.Val)
		if err != nil {
			return nil, err
		}
		return &Literal{Value: val}, nil
	case sqlparser.FloatVal:
		val, err := sqltypes.NewValue(sqltypes.Float64, expr.Val)
		if err != nil {
			return nil, err
		}
		return &Literal{Value: val}, nil
	case sqlparser.HexNum:
		if v, err := strconv.ParseUint(string(expr.Val), 16, 64); err != nil {
			return nil, err
		} else {
			return &Literal{Value: sqltypes.NewUint64(v)}, nil
		}
	case sqlparser.HexVal:
		if data, err := hex.DecodeString(string(expr.Val)); err != nil {
			return nil, err
		} else if val, err := sqltypes.NewValue(sqltypes.VarBinary, data); err != nil {
			return nil, err
		} else {
			return &Literal{Value: val}, nil
		}
	case sqlparser.ValArg:
		return &Bind{Name: ":" + string(expr.Val)}, nil
	case sqlparser.BitVal:
		if data, ok := new(big.Int).SetString(string(expr.Val), 2); !ok {
			return nil, fmt.Errorf("invalid bit string '%s'", expr.Val)
		} else if val, err := sqltypes.NewValue(sqltypes.Bit, data.Bytes()); err != nil {
			return nil, err
		} else {
			return &Literal{Value: val}, nil
		}
	}
	return nil, driver.ErrSkip
}

func (b *Builder) buildNullValue(_ *sqlparser.NullVal) (Expr, error) {
	return &Literal{Value: sqltypes.NULL}, nil
}

func (b *Builder) buildBoolVal(expr sqlparser.BoolVal) (Expr, error) {
	val := sqltypes.NewInt64(1)
	if expr {
		val = sqltypes.NewInt64(0)
	}
	return &Literal{Value: val}, nil
}

func (b *Builder) buildColName(expr *sqlparser.ColName) (Expr, error) {
	return &Column{Value: expr}, nil
}

func (b *Builder) buildValTuple(expr sqlparser.ValTuple) (Expr, error) {
	var exprs []Expr
	for _, val := range expr {
		elem, err := b.Build(val)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, elem)
	}
	return &Values{Exprs: exprs}, nil
}

func (b *Builder) buildListArg(expr sqlparser.ListArg) (Expr, error) {
	return &Bind{Name: "::" + string(expr)}, nil
}

func (b *Builder) buildBinaryExpr(expr *sqlparser.BinaryExpr) (Expr, error) {
	left, err := b.Build(expr.Left)
	if err != nil {
		return nil, err
	}
	right, err := b.Build(expr.Right)
	if err != nil {
		return nil, err
	}

	switch expr.Operator {
	case sqlparser.PlusStr:
		return &Plus{Left: left, Right: right}, nil
	case sqlparser.MinusStr:
		return &Minus{Left: left, Right: right}, nil
	case sqlparser.MultStr:
		return &Multiply{Left: left, Right: right}, nil
	case sqlparser.DivStr:
		return &Divide{Left: left, Right: right}, nil
	case sqlparser.IntDivStr:
		return &Divide{Left: left, Right: right}, nil
	case sqlparser.ModStr:
		return &Modulo{Left: left, Right: right}, nil
	case sqlparser.ShiftLeftStr:
		return &ShiftLeft{Left: left, Right: right}, nil
	case sqlparser.ShiftRightStr:
		return &ShiftRight{Left: left, Right: right}, nil
	case sqlparser.BitAndStr:
		return &BitAnd{Left: left, Right: right}, nil
	case sqlparser.BitOrStr:
		return &BitOr{Left: left, Right: right}, nil
	case sqlparser.BitXorStr:
		return &BitXor{Left: left, Right: right}, nil
	default:
		return nil, driver.ErrSkip
	}
}

func (b *Builder) buildUnaryExpr(expr *sqlparser.UnaryExpr) (Expr, error) {
	input, err := b.Build(expr.Expr)
	if err != nil {
		return nil, err
	}

	switch expr.Operator {
	case sqlparser.UPlusStr:
		return input, nil
	case sqlparser.UMinusStr:
		return &Multiply{Left: input, Right: &Literal{Value: sqltypes.NewInt64(-1)}}, nil
	case sqlparser.TildaStr:
		return &BitNot{Input: input}, nil
	case sqlparser.BangStr:
		return &Not{Input: input}, nil
	case sqlparser.BinaryStr, sqlparser.UBinaryStr:
		return &Convert{Input: input, Type: &sqlparser.ConvertType{Type: querypb.Type_name[int32(querypb.Type_VARBINARY)]}}, nil
	default:
		return nil, driver.ErrSkip
	}
}

func (b *Builder) buildIntervalExpr(expr *sqlparser.IntervalExpr) (Expr, error) {
	input, err := b.Build(expr.Expr)
	if err != nil {
		return nil, err
	}
	return &Interval{Input: input, Unit: expr.Unit}, nil
}

func (b *Builder) buildFuncExpr(expr *sqlparser.FuncExpr) (Expr, error) {
	exprs := make([]Expr, 0, len(expr.Exprs))
	for _, expr := range expr.Exprs {
		switch e := expr.(type) {
		case *sqlparser.StarExpr:
			exprs = append(exprs, &Columns{Table: e.TableName})
		case *sqlparser.AliasedExpr:
			expr, err := b.Build(e.Expr)
			if err != nil {
				return nil, err
			}
			exprs = append(exprs, expr)
		default:
			return nil, driver.ErrSkip
		}
	}

	input := Expr(&Values{Exprs: exprs})
	if expr.Distinct {
		input = &Distinct{Input: input}
	}
	return &Func{
		Dispatcher: b.dispatcher,
		Qualifier:  expr.Qualifier,
		Name:       expr.Name,
		Aggregate:  expr.IsAggregate(),
		Input:      input,
	}, nil
}

func (b *Builder) buildCaseExpr(expr *sqlparser.CaseExpr) (Expr, error) {
	when, err := b.Build(expr.Expr)
	if err != nil {
		return nil, err
	}
	right, err := b.Build(expr.Else)
	if err != nil {
		return nil, err
	}

	for i := len(expr.Whens) - 1; i >= 0; i-- {
		cond, err := b.Build(expr.Whens[i].Cond)
		if err != nil {
			return nil, err
		}
		then, err := b.Build(expr.Whens[i].Val)
		if err != nil {
			return nil, err
		}
		right = &If{
			When: &Equal{Left: when, Right: cond},
			Then: then,
			Else: right,
		}
	}
	return right, nil
}

func (b *Builder) buildValuesFuncExpr(expr *sqlparser.ValuesFuncExpr) (Expr, error) {
	return &Column{Value: expr.Name}, nil
}

func (b *Builder) buildConvertExpr(expr *sqlparser.ConvertExpr) (Expr, error) {
	input, err := b.Build(expr.Expr)
	if err != nil {
		return nil, err
	}
	return &Convert{Input: input, Type: expr.Type}, nil
}

func (b *Builder) buildSubstrExpr(expr *sqlparser.SubstrExpr) (Expr, error) {
	exprs := []Expr{&Column{Value: expr.Name}}
	if expr.From != nil {
		from, err := b.Build(expr.From)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, from)
	}
	if expr.To != nil {
		to, err := b.Build(expr.To)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, to)
	}
	return &Func{
		Dispatcher: b.dispatcher,
		Name:       sqlparser.NewColIdent("substr"),
		Input:      &Values{Exprs: exprs},
	}, nil
}

func (b *Builder) buildMatchExpr(expr *sqlparser.MatchExpr) (Expr, error) {
	columns := make([]Expr, 0, len(expr.Columns))
	for _, expr := range expr.Columns {
		switch e := expr.(type) {
		case *sqlparser.StarExpr:
			columns = append(columns, &Columns{Table: e.TableName})
		case *sqlparser.AliasedExpr:
			expr, err := b.Build(e.Expr)
			if err != nil {
				return nil, err
			}
			columns = append(columns, expr)
		default:
			return nil, driver.ErrSkip
		}
	}
	input, err := b.Build(expr.Expr)
	if err != nil {
		return nil, err
	}
	return &Match{
		Columns: columns,
		Input:   input,
		Option:  expr.Option,
	}, nil
}
