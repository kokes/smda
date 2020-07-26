package expr

import (
	"errors"
	"fmt"

	"github.com/kokes/smda/src/column"
)

var errQueryPatternNotSupported = errors.New("query pattern not supported")

// OPTIM/TODO: rename to sorted slice search and implement binary search (since we know colnames to be sorted)
func findInStringSlice(haystack []string, needle string) int {
	for j, el := range haystack {
		if el == needle {
			return j
		}
	}
	return -1
}

// OPTIM: consider replacing this with a map (O(log n) -> O(1), only some minor prep needed)
//        this would alter the Evaluate function signature - we'd accept map on the input
func getColumn(colName string, colNames []string, columns []column.Chunk) column.Chunk {
	return columns[findInStringSlice(colNames, colName)]
}

// OPTIM: we're doing a lot of type shenanigans at runtime - when we evaluate a function on each stripe, we do
// the same tree of operations - we could detect what functions/methods need to be called at parse time
// TODO: we don't do NULL handling in any of these - we should do this at once for all these operations,
//       but I can't think far ahead to know if null handling will be used everywhere or just in some cases,
//       e.g. in function calls we might want to know if a value is null, or in comparison with null literals
func Evaluate(expr *Expression, colnames []string, columns []column.Chunk) (column.Chunk, error) {
	switch expr.etype {
	case exprIdentifier:
		return getColumn(expr.value, colnames, columns), nil
	// case exprLiteralBool, exprLiteralFloat, exprLiteralInt, exprLiteralString: // TODO: expr.isLiteral?
	// 	...
	// case exprFunCall
	case exprEquality, exprNequality, exprLessThan, exprLessThanEqual, exprGreaterThan, exprGreaterThanEqual,
		exprAddition, exprSubtraction, exprMultiplication, exprDivision:
		c1, err := Evaluate(expr.children[0], colnames, columns)
		if err != nil {
			return nil, err
		}
		c2, err := Evaluate(expr.children[1], colnames, columns)
		if err != nil {
			return nil, err
		}
		// TODO: this would be a good place for NULL handling? though we don't have access to these fields
		// this handling would be best done in all these column.Eval{...} functions, but we don't have
		// one good place there to resolve nulls
		// bm := bitmap.Or(c1.Nullability, c2.Nullability)
		switch expr.etype {
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
		}
		fallthrough
	default:
		return nil, fmt.Errorf("expression %v not supported: %w", expr, errQueryPatternNotSupported)
	}
}
