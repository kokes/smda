// steps to add a new function:
// 1. add an implementation here
// 2. specify its return types (return_types.go)
// 3. dispatch it in Evaluate (eval.go) - may be done in the parser at some point
// 4. test all three implementations above
package column

import (
	"errors"
	"fmt"
	"math"
)

var errTypeNotSupported = errors.New("type not supported in this function")
var errNotImplemented = errors.New("not implemented yet")

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

// ARCH: this could be generalised using numFunc, we just have to pass in a closure
// with our power
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

func numFunc(fnc func(float64) float64) func(...Chunk) (Chunk, error) {
	return func(cs ...Chunk) (Chunk, error) {
		switch ct := cs[0].(type) {
		case *ChunkInts:
			// TODO: cast to floats and apply func
			return nil, errNotImplemented // TODO
		case *ChunkFloats:
			ctr := ct.Clone().(*ChunkFloats)
			for j, el := range ctr.data {
				// TODO: nanify (nan -> set null)
				ctr.data[j] = fnc(el)
			}
			return ctr, nil
		default:
			return nil, fmt.Errorf("%w: func(%v)", errTypeNotSupported, ct.Dtype())
		}
	}
}

// TODO: rest of these functions
var EvalSin = numFunc(math.Sin)
var EvalCos = numFunc(math.Cos)
var EvalTan = numFunc(math.Tan)
var EvalExp = numFunc(math.Exp)
var EvalExp2 = numFunc(math.Exp2)
var EvalLog = numFunc(math.Log)
var EvalLog2 = numFunc(math.Log2)
var EvalLog10 = numFunc(math.Log10)

// var EvalAbs = numFunc(math.Abs) // this should probably behave differently for ints
