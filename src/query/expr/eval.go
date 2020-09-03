package expr

import (
	"errors"
	"fmt"

	"github.com/kokes/smda/src/column"
)

var errQueryPatternNotSupported = errors.New("query pattern not supported")

// OPTIM: we're doing a lot of type shenanigans at runtime - when we evaluate a function on each stripe, we do
// the same tree of operations - we could detect what functions/methods need to be called at parse time
func Evaluate(expr *Expression, columnData map[string]column.Chunk) (column.Chunk, error) {
	switch expr.etype {
	case exprIdentifier:
		col, ok := columnData[expr.value]
		if !ok {
			// we validated the expression, so this should not happen?
			// perhaps to catch bugs in case folding?
			return nil, fmt.Errorf("column %v not found", expr.value)
		}
		return col, nil
	// TODO: there's no way of knowing the length now in any of the literal cases
	// we'll need to pass in stripe length into Evaluate to deal with these cases
	case exprLiteralBool:
		return column.NewChunkLiteralTyped(expr.value, column.DtypeBool, 0), nil
	case exprLiteralFloat:
		return column.NewChunkLiteralTyped(expr.value, column.DtypeFloat, 0), nil
	case exprLiteralInt:
		return column.NewChunkLiteralTyped(expr.value, column.DtypeInt, 0), nil
	case exprLiteralString:
		return column.NewChunkLiteralTyped(expr.value, column.DtypeString, 0), nil
	// null is not a literal type yet
	// case exprLiteralNull:
	// 	return column.NewChunkLiteralTyped(expr.value, column.DtypeBool, 0), nil
	// case exprFunCall
	case exprEquality, exprNequality, exprLessThan, exprLessThanEqual, exprGreaterThan, exprGreaterThanEqual,
		exprAddition, exprSubtraction, exprMultiplication, exprDivision, exprAnd, exprOr:
		c1, err := Evaluate(expr.children[0], columnData)
		if err != nil {
			return nil, err
		}
		c2, err := Evaluate(expr.children[1], columnData)
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
		}
		fallthrough
	default:
		return nil, fmt.Errorf("expression %v not supported: %w", expr, errQueryPatternNotSupported)
	}
}
