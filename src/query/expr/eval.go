package expr

import (
	"errors"
	"fmt"

	"github.com/kokes/smda/src/column"
)

var errQueryPatternNotSupported = errors.New("query pattern not supported")
var errFunctionNotImplemented = errors.New("function not implemented")

// OPTIM: we're doing a lot of type shenanigans at runtime - when we evaluate a function on each stripe, we do
// the same tree of operations - this applies not just here, but in projections.go as well - e.g. we know that
// if we have `intA - intB`, we'll run a function for ints - we don't need to decide that for each stripe
func Evaluate(expr *Expression, chunkLength int, columnData map[string]column.Chunk) (column.Chunk, error) {
	// TODO: test this via UpdateAggregator
	if expr.aggregator != nil {
		return expr.aggregator.Resolve()
	}
	switch expr.etype {
	case exprIdentifier:
		col, ok := columnData[expr.value]
		if !ok {
			// we validated the expression, so this should not happen?
			// perhaps to catch bugs in case folding?
			return nil, fmt.Errorf("column %v not found", expr.value)
		}
		return col, nil
	// since these literals don't interact with any "dense" column chunks, we need
	// to pass in their lengths
	case exprLiteralBool:
		return column.NewChunkLiteralTyped(expr.value, column.DtypeBool, chunkLength)
	case exprLiteralFloat:
		return column.NewChunkLiteralTyped(expr.value, column.DtypeFloat, chunkLength)
	case exprLiteralInt:
		return column.NewChunkLiteralTyped(expr.value, column.DtypeInt, chunkLength)
	case exprLiteralString:
		return column.NewChunkLiteralTyped(expr.value, column.DtypeString, chunkLength)
	// null is not a literal type yet
	// case exprLiteralNull:
	// 	return column.NewChunkLiteralTyped(expr.value, column.DtypeBool, 0)
	// OPTIM/TODO: we could optimise shallow function calls - e.g. `log(foo) > 1` doesn't need
	// `log(foo)` as a newly allocated chunk, we can compute that on the fly
	case exprFunCall:
		if expr.evaler == nil {
			return nil, fmt.Errorf("%w: %s", errFunctionNotImplemented, expr.value)
		}
		// ARCH: abstract out this `children` construction and use it elsewhere (in exprEquality etc.)
		children := make([]column.Chunk, 0, len(expr.children))
		for _, ch := range expr.children {
			child, err := Evaluate(ch, chunkLength, columnData)
			if err != nil {
				return nil, err
			}
			children = append(children, child)
		}
		return expr.evaler(children...)
	// ARCH: these could all be generalised as FunCalls
	case exprEquality, exprNequality, exprLessThan, exprLessThanEqual, exprGreaterThan, exprGreaterThanEqual,
		exprAddition, exprSubtraction, exprMultiplication, exprDivision, exprAnd, exprOr:
		c1, err := Evaluate(expr.children[0], chunkLength, columnData)
		if err != nil {
			return nil, err
		}
		c2, err := Evaluate(expr.children[1], chunkLength, columnData)
		if err != nil {
			return nil, err
		}
		switch expr.etype {
		case exprAnd:
			return column.EvalAnd(c1, c2)
		case exprOr:
			return column.EvalOr(c1, c2)
		case exprEquality:
			return column.EvalEq(c1, c2)
		case exprNequality:
			return column.EvalNeq(c1, c2)
		case exprLessThan:
			return column.EvalLt(c1, c2)
		case exprLessThanEqual:
			return column.EvalLte(c1, c2)
		case exprGreaterThan:
			return column.EvalGt(c1, c2)
		case exprGreaterThanEqual:
			return column.EvalGte(c1, c2)
		case exprAddition:
			return column.EvalAdd(c1, c2)
		case exprSubtraction:
			return column.EvalSubtract(c1, c2)
		case exprDivision:
			return column.EvalDivide(c1, c2)
		case exprMultiplication:
			return column.EvalMultiply(c1, c2)
		}
		fallthrough
	default:
		return nil, fmt.Errorf("expression %v not supported: %w", expr, errQueryPatternNotSupported)
	}
}

func UpdateAggregator(expr *Expression, buckets []uint64, ndistinct int, columnData map[string]column.Chunk) error {
	// if expr.aggregator == nil {err}
	// if len(expr.children) != 1 {err}// what about count()?

	// e.g. sum(1+foo) needs `1+foo` evaluated first, then we feed the resulting
	// chunk to the sum aggregator
	var child column.Chunk
	var err error
	// in case we have e.g. `count()`, we cannot evaluate its children as there are none
	if len(expr.children) > 0 {
		child, err = Evaluate(expr.children[0], len(buckets), columnData)
		if err != nil {
			return err
		}
	}
	expr.aggregator.AddChunk(buckets, ndistinct, child)
	return nil
}
