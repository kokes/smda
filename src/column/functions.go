// steps to add a new function:
// 1. add an implementation here
// 2. specify its return types (return_types.go)
// 3. dispatch it in Evaluate (eval.go)
// 4. test all three implementations above
package column

import (
	"errors"
	"fmt"
	"math"
)

var errTypeNotSupported = errors.New("type not supported in this function")

// at some point test sum(nullif([1,2,3], 2)) to check we're not interpreting
// "dead" values
// treat this differently, if cs[0] is a literal column
func EvalNullIf(cs ...Chunk) (Chunk, error) {
	eq, err := EvalEq(cs[0], cs[1])
	if err != nil {
		return nil, err
	}
	truths := eq.(*ChunkBools).Truths()
	if truths.Count() == 0 {
		return cs[0], nil
	}
	cb := cs[0].Clone()
	cb.Nullify(truths)
	return cb, nil
}

func EvalRound(cs ...Chunk) (Chunk, error) {
	factor := cs[1].(*ChunkInts).data[0] // TODO: check factor size (and test it)
	pow := math.Pow10(int(factor))
	switch ct := cs[0].(type) {
	case *ChunkInts:
		// TODO: cast to floats and do nothing
		return nil, fmt.Errorf("%w: %v", errTypeNotSupported, ct.Dtype())
	case *ChunkFloats:
		ctr := ct.Clone().(*ChunkFloats)
		for j, el := range ctr.data {
			// TODO: is this the right way to round to n digits? What about overflows or loss of precision?
			ctr.data[j] = math.Round(pow*el) / pow
		}
		return ctr, nil
	default:
		return nil, fmt.Errorf("%w: round(%v)", errTypeNotSupported, ct.Dtype())
	}
}
