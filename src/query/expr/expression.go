package expr

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/kokes/smda/src/column"
)

var errNoNestedAggregations = errors.New("cannot nest aggregations (e.g. sum(min(a)))")

type exprType uint8

const (
	exprInvalid exprType = iota
	exprIdentifier
	exprIdentifierQuoted
	exprAnd
	exprOr
	exprAddition
	exprSubtraction
	exprMultiplication
	exprDivision
	exprEquality
	exprNequality
	exprLessThan
	exprLessThanEqual
	exprGreaterThan
	exprGreaterThanEqual
	exprLiteralInt
	exprLiteralFloat
	exprLiteralBool
	exprLiteralString
	exprLiteralNull
	exprFunCall
)

func (expr *Expression) IsIdentifier() bool {
	return expr.etype == exprIdentifier || expr.etype == exprIdentifierQuoted
}
func (expr *Expression) IsOperatorBoolean() bool {
	return expr.etype == exprAnd || expr.etype == exprOr
}
func (expr *Expression) IsOperatorMath() bool {
	return expr.etype >= exprAddition && expr.etype <= exprDivision
}
func (expr *Expression) IsOperatorComparison() bool {
	return expr.etype >= exprEquality && expr.etype <= exprGreaterThanEqual
}

func (expr *Expression) IsOperator() bool {
	return expr.IsOperatorBoolean() || expr.IsOperatorMath() || expr.IsOperatorComparison()
}
func (expr *Expression) IsLiteral() bool {
	return expr.etype >= exprLiteralInt && expr.etype <= exprLiteralNull
}

// ARCH: is this used anywhere? (partially in the Expression stringer, partially in error reporting in parser_test.go)
func (etype exprType) String() string {
	switch etype {
	case exprInvalid:
		return "Invalid"
	case exprIdentifier:
		return "Identifier"
	case exprIdentifierQuoted:
		return "QuotedIdentifier"
	case exprAnd:
		return "AND"
	case exprOr:
		return "OR"
	case exprAddition:
		return "+"
	case exprSubtraction:
		return "-"
	case exprMultiplication:
		return "*"
	case exprDivision:
		return "/"
	case exprEquality:
		return "="
	case exprNequality:
		return "!="
	case exprLessThan:
		return "<"
	case exprLessThanEqual:
		return "<="
	case exprGreaterThan:
		return ">"
	case exprGreaterThanEqual:
		return ">="
	case exprLiteralInt:
		return "LiteralInt"
	case exprLiteralFloat:
		return "LiteralFloat"
	case exprLiteralBool:
		return "LiteralBool"
	case exprLiteralString:
		return "LiteralString"
	case exprLiteralNull:
		return "LiteralNull"
	case exprFunCall:
		return "FunCall"
	default:
		return "unknown_expression"
	}
}

type Expression struct {
	etype             exprType
	children          []*Expression
	value             string
	parens            bool
	evaler            func(...column.Chunk) (column.Chunk, error)
	aggregator        *column.AggState
	aggregatorFactory func(...column.Dtype) (*column.AggState, error)
}

func (expr *Expression) InitAggregator(schema column.TableSchema) error {
	var rtypes []column.Dtype
	for _, ch := range expr.children {
		rtype, err := ch.ReturnType(schema)
		if err != nil {
			return err
		}
		rtypes = append(rtypes, rtype.Dtype)
	}
	aggregator, err := expr.aggregatorFactory(rtypes...)
	if err != nil {
		return err
	}
	expr.aggregator = aggregator
	return nil
}

func AggExpr(expr *Expression) ([]*Expression, error) {
	var ret []*Expression
	found := false
	if expr.etype == exprFunCall && expr.evaler == nil {
		ret = append(ret, expr)
		found = true
	}
	for _, ch := range expr.children {
		ach, err := AggExpr(ch)
		if err != nil {
			return nil, err
		}
		if ach != nil {
			if found {
				return nil, errNoNestedAggregations
			}
			ret = append(ret, ach...)
		}
	}
	return ret, nil
}

func (expr *Expression) String() string {
	var rval string
	switch expr.etype {
	case exprInvalid:
		rval = "invalid_expression"
	case exprIdentifier:
		rval = expr.value
	case exprIdentifierQuoted:
		rval = fmt.Sprintf("\"%s\"", expr.value)
	case exprAddition, exprSubtraction, exprMultiplication, exprDivision, exprEquality,
		exprNequality, exprLessThan, exprLessThanEqual, exprGreaterThan, exprGreaterThanEqual, exprAnd, exprOr:
		rval = fmt.Sprintf("%s %s %s", expr.children[0], expr.etype, expr.children[1])
	case exprLiteralInt, exprLiteralFloat, exprLiteralBool:
		rval = expr.value
	case exprLiteralString:
		// TODO: what about literals with apostrophes in them? escape them
		rval = fmt.Sprintf("'%s'", expr.value)
	case exprLiteralNull:
		rval = "NULL"
	case exprFunCall:
		args := make([]string, 0, len(expr.children))
		for _, ch := range expr.children {
			args = append(args, ch.String())
		}

		rval = fmt.Sprintf("%s(%s)", expr.value, strings.Join(args, ", "))
	default:
		// we need to panic, because we use this stringer for expression comparison
		panic(fmt.Sprintf("unsupported expression type: %v", expr.etype))
	}
	if expr.parens {
		return fmt.Sprintf("(%s)", rval)
	}
	return rval
}

func (expr *Expression) UnmarshalJSON(data []byte) error {
	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	ex, err := ParseStringExpr(raw)
	if ex != nil {
		*expr = *ex
	}
	return err
}

func (expr *Expression) MarshalJSON() ([]byte, error) {
	return json.Marshal(expr.String())
}
