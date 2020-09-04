package expr

import (
	"errors"
	"fmt"

	"github.com/kokes/smda/src/column"
)

var errQueryPatternNotSupported = errors.New("query pattern not supported")

// OPTIM: we're doing a lot of type shenanigans at runtime - when we evaluate a function on each stripe, we do
// the same tree of operations - we could detect what functions/methods need to be called at parse time
// we could save these functions within the expression - as long as they have the same signature (we're trying
// to adehere to func(...Chunk) (Chunk, error))
// Also, we can construct some of these beforehand - like the generated sin/cos/acos/... functions
//  - we know about these function calls at parse time, so just assign function calls there
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
	case exprFunCall:
		// ARCH: abstract out this `children` construction and use it elsewhere (in exprEquality etc.)
		children := make([]column.Chunk, 0, len(expr.children))
		for _, ch := range expr.children {
			child, err := Evaluate(ch, columnData)
			if err != nil {
				return nil, err
			}
			children = append(children, child)
		}
		switch expr.value {
		case "nullif":
			return column.EvalNullIf(children...)
		case "round":
			return column.EvalRound(children...)
		case "sin":
			return column.EvalSin(children...)
		case "cos":
			return column.EvalCos(children...)
		case "tan":
			return column.EvalTan(children...)
		case "exp":
			return column.EvalExp(children...)
		case "exp2":
			return column.EvalExp2(children...)
		case "log":
			return column.EvalLog(children...)
		case "log2":
			return column.EvalLog2(children...)
		case "log10":
			return column.EvalLog10(children...)
		default:
			return nil, fmt.Errorf("function %v not supported", expr.value)
		}
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
