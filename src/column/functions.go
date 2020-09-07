// steps to add a new function:
// 1. add an implementation here and add it to FuncProj
// 2. specify its return types (return_types.go)
// 3. test both implementations above
package column

import (
	"errors"
	"fmt"
	"math"
)

var errTypeNotSupported = errors.New("type not supported in this function")
var errNotImplemented = errors.New("not implemented yet")

// TODO: this will be hard to cover properly, so let's make sure we test everything explicitly
var FuncProj = map[string]func(...Chunk) (Chunk, error){
	"nullif": EvalNullIf,
	"round":  EvalRound,
	"sin":    numFunc(math.Sin),
	"cos":    numFunc(math.Cos),
	"tan":    numFunc(math.Tan),
	"exp":    numFunc(math.Exp),
	"exp2":   numFunc(math.Exp2),
	"log":    numFunc(math.Log),
	"log2":   numFunc(math.Log2),
	"log10":  numFunc(math.Log10),
	// TODO: these are just placeholders for parser tests to pass
	"coalesce": nil,
}

// var EvalAbs = numFunc(math.Abs) // this should probably behave differently for ints

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
				// though math.Log(0) -> -Inf, set that to NaN as well?
				ctr.data[j] = fnc(el)
			}
			return ctr, nil
		default:
			return nil, fmt.Errorf("%w: func(%v)", errTypeNotSupported, ct.Dtype())
		}
	}
}
