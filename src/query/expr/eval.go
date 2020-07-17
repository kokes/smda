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
func Evaluate(expr *Expression, colnames []string, columns []column.Chunk) (column.Chunk, error) {
	// bm := bitmap.Or(c1.Nullability, c2.Nullability)
	switch expr.etype {
	case exprIdentifier:
		return getColumn(expr.value, colnames, columns), nil
	case exprEquality:
		c1, err := Evaluate(expr.children[0], colnames, columns)
		if err != nil {
			return nil, err
		}
		c2, err := Evaluate(expr.children[0], colnames, columns)
		if err != nil {
			return nil, err
		}
		return column.EvalEq(c1, c2)
	default:
		return nil, fmt.Errorf("expression %v not supported: %w", expr, errQueryPatternNotSupported)
	}
}
