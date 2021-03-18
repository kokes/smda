package expr

import (
	"errors"
	"fmt"
	"strings"

	"github.com/kokes/smda/src/bitmap"
	"github.com/kokes/smda/src/column"
)

var errQueryPatternNotSupported = errors.New("query pattern not supported")
var errFunctionNotImplemented = errors.New("function not implemented")

// OPTIM: we're doing a lot of type shenanigans at runtime - when we evaluate a function on each stripe, we do
// the same tree of operations - this applies not just here, but in projections.go as well - e.g. we know that
// if we have `intA - intB`, we'll run a function for ints - we don't need to decide that for each stripe
func Evaluate(expr *Expression, chunkLength int, columnData map[string]column.Chunk, filter *bitmap.Bitmap) (column.Chunk, error) {
	// TODO: test this via UpdateAggregator
	if expr.aggregator != nil {
		// TODO: assert that filters !== nil?
		return expr.aggregator.Resolve()
	}
	switch expr.etype {
	// ARCH: perhaps use expr.IsIdentifier?
	case exprIdentifier, exprIdentifierQuoted:
		lookupValue := expr.value
		if expr.etype == exprIdentifier {
			lookupValue = strings.ToLower(lookupValue)
		}
		col, ok := columnData[lookupValue]
		if !ok {
			// we validated the expression, so this should not happen?
			// perhaps to catch bugs in case folding?
			return nil, fmt.Errorf("column %v not found", expr.value)
		}
		if filter != nil {
			return col.Prune(filter), nil
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
	case exprLiteralNull:
		return column.NewChunkLiteralTyped("", column.DtypeNull, chunkLength)
	// OPTIM: we could optimise shallow function calls - e.g. `log(foo) > 1` doesn't need
	// `log(foo)` as a newly allocated chunk, we can compute that on the fly
	case exprFunCall:
		// TODO(next): if we do count() or sum(foo) without aggregations, this should
		// run on the whole dataset - currently triggers this error
		if expr.evaler == nil {
			return nil, fmt.Errorf("%w: %s", errFunctionNotImplemented, expr.value)
		}
		// ARCH: abstract out this `children` construction and use it elsewhere (in exprEquality etc.)
		children := make([]column.Chunk, 0, len(expr.children))
		for _, ch := range expr.children {
			child, err := Evaluate(ch, chunkLength, columnData, filter)
			if err != nil {
				return nil, err
			}
			children = append(children, child)
		}
		return expr.evaler(children...)
	// ARCH: these could all be generalised as FunCalls
	case exprEquality, exprNequality, exprLessThan, exprLessThanEqual, exprGreaterThan, exprGreaterThanEqual,
		exprAddition, exprSubtraction, exprMultiplication, exprDivision, exprAnd, exprOr:
		c1, err := Evaluate(expr.children[0], chunkLength, columnData, filter)
		if err != nil {
			return nil, err
		}
		c2, err := Evaluate(expr.children[1], chunkLength, columnData, filter)
		if err != nil {
			return nil, err
		}

		// TODO(next): test null=null, null>null (in filters, groupbys, selects, wherever)
		if c1.Dtype() == column.DtypeNull && c2.Dtype() == column.DtypeNull {
			return nil, errQueryPatternNotSupported // ARCH: wrap?
		}

		// there are a few cases to consider here:
		// a) which one is null, if it the first or the second (luckily our key functions are commutative)
		// b) is this a literal null or a "fat" null? Maybe it doesn't matter...
		// ARCH: what should `2-NULL` be in terms of types? Is this dtypenull or dtypeint [with all nulls]? Check pg or other engines
		if c1.Dtype() == column.DtypeNull || c2.Dtype() == column.DtypeNull {
			// TODO(next): implement this
			// sadly c1.base is not accessible, so we cannot tell anything about the nullability here
			// for eq - we could just invert the nullability bitmap (be careful about literal chunks - those will be literal bools then)
			// for neq... we'll just copy the nullability bitmap to the values bitmap
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
		return nil, fmt.Errorf("expression %v (%v) not supported: %w", expr, expr.etype, errQueryPatternNotSupported)
	}
}

func UpdateAggregator(expr *Expression, buckets []uint64, ndistinct int, columnData map[string]column.Chunk, filter *bitmap.Bitmap) error {
	// if expr.aggregator == nil {err}
	// if len(expr.children) != 1 {err}// what about count()?

	// e.g. sum(1+foo) needs `1+foo` evaluated first, then we feed the resulting
	// chunk to the sum aggregator
	var child column.Chunk
	var err error
	// in case we have e.g. `count()`, we cannot evaluate its children as there are none
	if len(expr.children) > 0 {
		child, err = Evaluate(expr.children[0], len(buckets), columnData, filter)
		if err != nil {
			return err
		}
	}
	expr.aggregator.AddChunk(buckets, ndistinct, child)
	return nil
}
