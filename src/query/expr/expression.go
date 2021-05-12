package expr

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/kokes/smda/src/column"
	"github.com/kokes/smda/src/database"
)

var errNoNestedAggregations = errors.New("cannot nest aggregations (e.g. sum(min(a)))")

type exprType uint8

const (
	exprInvalid exprType = iota
	exprIdentifier
	exprIdentifierQuoted
	exprRelabel
	exprSort
	exprUnaryMinus
	exprNot
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

var (
	sortAscNullsFirst  = "ASC NULLS FIRST"
	sortAscNullsLast   = "ASC NULLS LAST"
	sortDescNullsFirst = "DESC NULLS FIRST"
	sortDescNullsLast  = "DESC NULLS LAST"
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
	case exprRelabel:
		return "AS"
	case exprSort:
		return "ASC/DESC"
	case exprUnaryMinus:
		return "UnaryMinus"
	case exprNot:
		return "NOT"
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

type ExpressionList []*Expression

// Query describes what we want to retrieve from a given dataset
// There are basically four places you need to edit (and test!) in order to extend this:
// 1) The engine itself needs to support this functionality (usually a method on Dataset or column.Chunk)
// 2) The query method has to be able to translate query parameters to the engine
// 3) The query endpoint handler needs to be able to process the incoming body
//    to the Query struct (the Unmarshaler should mostly take care of this)
// 4) The HTML/JS frontend needs to incorporate this in some way
type Query struct {
	Select    ExpressionList              `json:"select,omitempty"`
	Dataset   *database.DatasetIdentifier `json:"dataset"`
	Filter    *Expression                 `json:"filter,omitempty"`
	Aggregate ExpressionList              `json:"aggregate,omitempty"`
	Order     ExpressionList              `json:"order,omitempty"`
	Limit     *int                        `json:"limit,omitempty"`
	// TODO: PAFilter (post-aggregation filter, == having) - check how it behaves without aggregations elsewhere
}

// this stringer is tested in the parser
func (q Query) String() string {
	var sb strings.Builder
	sb.WriteString("SELECT ")
	sb.WriteString(q.Select.String())
	// ARCH: preparing for queries without FROM clauses
	if q.Dataset != nil {
		sb.WriteString(fmt.Sprintf(" FROM %s", q.Dataset))
	}
	if q.Filter != nil {
		sb.WriteString(fmt.Sprintf(" WHERE %s", q.Filter))
	}
	if q.Aggregate != nil {
		sb.WriteString(fmt.Sprintf(" GROUP BY %s", q.Aggregate))
	}
	if q.Order != nil {
		sb.WriteString(fmt.Sprintf(" ORDER BY %s", q.Aggregate))
	}
	if q.Limit != nil {
		sb.WriteString(fmt.Sprintf(" LIMIT %d", *q.Limit))
	}

	return sb.String()
}

func (expr *Expression) InitFunctionCalls() error {
	for _, ch := range expr.children {
		if err := ch.InitFunctionCalls(); err != nil {
			return err
		}
	}

	if expr.etype != exprFunCall {
		return nil
	}

	funName := expr.value
	fncp, ok := column.FuncProj[funName]
	if ok {
		expr.evaler = fncp
	} else {
		// if it's not a projection, it must be an aggregator
		// ARCH: cannot initialise the aggregator here, because we don't know
		// the types that go in (and we're already using static dispatch here)
		// TODO/ARCH: but since we've decoupled this from the parser, we might have the schema at hand already!
		//            we just need to remove this `InitFunctionCalls` from ParseStringExpr
		aggfac, err := column.NewAggregator(funName)
		if err != nil {
			return err
		}
		expr.aggregatorFactory = aggfac
	}

	return nil
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
	// ARCH: we used to test `expr.evaler == nil` in the second condition... better?
	if expr.etype == exprFunCall && expr.aggregatorFactory != nil {
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
	case exprRelabel:
		rval = fmt.Sprintf("%s AS %s", expr.children[0], expr.children[1])
	case exprNot:
		rval = fmt.Sprintf("NOT %s", expr.children[0])
	case exprSort:
		rval = fmt.Sprintf("%s %s", expr.children[0], expr.value)
	case exprUnaryMinus:
		rval = fmt.Sprintf("-%s", expr.children[0])
	case exprAddition, exprSubtraction, exprMultiplication, exprDivision, exprEquality,
		exprNequality, exprLessThan, exprLessThanEqual, exprGreaterThan, exprGreaterThanEqual, exprAnd, exprOr:
		// ARCH: maybe do `%s %s %s` for all of these, it will make our queries more readable
		if expr.etype == exprAnd || expr.etype == exprOr {
			rval = fmt.Sprintf("%s %s %s", expr.children[0], expr.etype, expr.children[1])
		} else {
			rval = fmt.Sprintf("%s%s%s", expr.children[0], expr.etype, expr.children[1])
		}
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

// ARCH: this is a bit contentious - our []*Expression aka ExpressionList (un)marshals
// as a "expr, expr2", NOT as "[]*Expression{expr, expr2}"
func (exprs *ExpressionList) UnmarshalJSON(data []byte) error {
	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	ex, err := ParseStringExprs(raw)
	if ex != nil {
		*exprs = ex
	}
	return err
}

func (exprs ExpressionList) String() string {
	var buf bytes.Buffer
	for j, expr := range exprs {
		buf.WriteString(expr.String())
		if j < len(exprs)-1 {
			buf.WriteString(", ")
		}
	}
	return buf.String()
}

func (exprs ExpressionList) MarshalJSON() ([]byte, error) {
	return json.Marshal(exprs.String())
}
