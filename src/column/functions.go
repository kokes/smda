// steps to add a new function:
// 1. add an implementation here and add it to FuncProj
// 2. specify its return types (return_types.go)
// 3. test both implementations above
package column

import (
	"errors"
	"fmt"
	"math"

	"github.com/kokes/smda/src/bitmap"
)

var errTypeNotSupported = errors.New("type not supported in this function")

// TODO: this will be hard to cover properly, so let's make sure we test everything explicitly
// ARCH: we're not treating literals any differently, but since they share the same backing store
//       as non-literals, we're okay... is that okay?
var FuncProj = map[string]func(...Chunk) (Chunk, error){
	"nullif":   EvalNullIf,
	"coalesce": EvalCoalesce,
	"round":    EvalRound, // TODO: ceil, floor
	"sin":      numFunc(math.Sin),
	"cos":      numFunc(math.Cos),
	"tan":      numFunc(math.Tan),
	"asin":     numFunc(math.Asin),
	"acos":     numFunc(math.Acos),
	"atan":     numFunc(math.Atan),
	"sinh":     numFunc(math.Sinh),
	"cosh":     numFunc(math.Cosh),
	"tanh":     numFunc(math.Tanh),
	"sqrt":     numFunc(math.Sqrt),
	"exp":      numFunc(math.Exp),
	"exp2":     numFunc(math.Exp2),
	"log":      numFunc(math.Log),
	"log2":     numFunc(math.Log2),
	"log10":    numFunc(math.Log10),
	// TODO: log with arbitrary base
}

func EvalCoalesce(cs ...Chunk) (Chunk, error) {
	if len(cs) == 0 {
		// ARCH: this should have been taken care of in return_types
		return nil, errors.New("coalesce needs at least one argument")
	}
	if len(cs) == 1 {
		return cs[0], nil
	}
	// OPTIM: if cs[0].IsNullable == false, exit with it (we don't have that method though)
	panic("TODO: not implemented yet")
	// how will we know the schema of this result? should we incorporate the return_type flow here?
	// I guess we can't do that since that would introduce a circular dependency
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
// ARCH: should this return decimals (which we don't support)?
func EvalRound(cs ...Chunk) (Chunk, error) {
	var factor int
	if len(cs) == 2 {
		// TODO: check factor size (and test it)
		factor = int(cs[1].(*ChunkInts).data[0])
	}
	pow := math.Pow10(factor)
	switch ct := cs[0].(type) {
	case *ChunkInts:
		// cast to floats and do nothing (nothing happens, regardless of the factor specified)
		return ct.cast(DtypeFloat)
	case *ChunkFloats:
		ctr := ct.Clone().(*ChunkFloats)
		for j, el := range ctr.data {
			// TODO: is this the right way to round to n digits? What about overflows or loss of precision?
			// ew can easily check by checking that abs(old-new) < 1
			// OPTIM: in case of factor == 0, we're doing meaningless multiplication and divison, consider
			//		  an extra if block outside this loop (at the cost of more code)
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
			rc, err := ct.cast(DtypeFloat)
			if err != nil {
				return nil, err
			}
			ctr := rc.(*ChunkFloats)
			for j, el := range ctr.data {
				val := fnc(el)
				if math.IsNaN(val) || math.IsInf(val, 0) {
					if ctr.nullability == nil {
						ctr.nullability = bitmap.NewBitmap(ctr.Len())
					}
					ctr.nullability.Set(j, true)
				}
				ctr.data[j] = val
			}
			return ctr, nil
		case *ChunkFloats:
			ctr := ct.Clone().(*ChunkFloats)
			for j, el := range ctr.data {
				val := fnc(el)
				if math.IsNaN(val) || math.IsInf(val, 0) {
					if ctr.nullability == nil {
						ctr.nullability = bitmap.NewBitmap(ctr.Len())
					}
					ctr.nullability.Set(j, true)
				}
				ctr.data[j] = val
			}
			return ctr, nil
		default:
			return nil, fmt.Errorf("%w: func(%v)", errTypeNotSupported, ct.Dtype())
		}
	}
}

// date_part/date_trunc (tho trunc returns a timestamp we don't support yet)
// century
// day
// decade
// dow
// doy
// epoch
// hour
// isodow
// isoyear
// microseconds
// millennium
// milliseconds
// minute
// month
// quarter
// second
// timezone
// timezone_hour
// timezone_minute
// week
// year
