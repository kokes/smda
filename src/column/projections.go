package column

import (
	"errors"
	"fmt"
)

var errProjectionNotSupported = errors.New("projection not supported")

// one thing that might help us with all the implementations of functions with 2+ arguments:
// sort them by dtypes (if possible!), that way we can implement far fewer cases
// in some cases (e.g. equality), we can simply swap the arguments
// in other cases (e.g. greater than), we need to swap the operator as well

// also, where will we deal with nulls? this should be in as few places as possible

// when this gets too long, split it up into projections_string, projections_date etc.

// OPTIM: what is c1 === c2? short circuit it with a boolean array (copy in the nullability vector though)
func EvalEq(c1 Chunk, c2 Chunk) (Chunk, error) {
	if c1.Dtype() != c2.Dtype() {
		// this includes int == float!
		// sort dtypes when implementing this (see the note above)
		return nil, fmt.Errorf("expression %v=%v not supported: %w", c1, c2, errProjectionNotSupported)
	}

	switch c1.Dtype() {
	case DtypeString:
		return evalEqStrings(c1.(*ChunkStrings), c2.(*ChunkStrings))
	default:
		return nil, fmt.Errorf("expression %v=%v not supported for types %v, %v: %w", c1, c2, c1.Dtype(), c2.Dtype(), errProjectionNotSupported)
	}

	return nil, nil
}

func evalEqStrings(c1 *ChunkStrings, c2 *ChunkStrings) (Chunk, error) {
	return nil, nil
}
