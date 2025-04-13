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
}

func NewBuilder() *Builder {
	return &Builder{}
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
	case *sqlparser.IsExpr:
	case *sqlparser.ExistsExpr:
	case *sqlparser.SQLVal:
		return b.buildSQLVal(expr)
	case *sqlparser.NullVal:
		return b.buildNullValue(expr)
	case sqlparser.BoolVal:
		return b.buildBoolVal(expr)
	case *sqlparser.ColName:
		return b.buildColName(expr)
	case sqlparser.ValTuple:
	case sqlparser.ListArg:
	case *sqlparser.BinaryExpr:
	case *sqlparser.UnaryExpr:
	case *sqlparser.IntervalExpr:
	case *sqlparser.CollateExpr:
	case *sqlparser.FuncExpr:
	case *sqlparser.CaseExpr:
	case *sqlparser.ValuesFuncExpr:
	case *sqlparser.ConvertExpr:
	case *sqlparser.SubstrExpr:
	case *sqlparser.ConvertUsingExpr:
	case *sqlparser.MatchExpr:
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
		return nil, fmt.Errorf("unsupported comparison operator %s", expr.Operator)
	}
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
