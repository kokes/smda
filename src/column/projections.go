package column

import (
	"errors"
	"fmt"

	"github.com/kokes/smda/src/bitmap"
)

var errProjectionNotSupported = errors.New("projection not supported")

// one thing that might help us with all the implementations of functions with 2+ arguments:
// sort them by dtypes (if possible!), that way we can implement far fewer cases
// in some cases (e.g. equality), we can simply swap the arguments
// in other cases (e.g. greater than), we need to swap the operator as well

// also, where will we deal with nulls? this should be in as few places as possible

// when this gets too long, split it up into projections_string, projections_date etc.

func compFactoryStrings(c1 *ChunkStrings, c2 *ChunkStrings, compFn func(string, string) bool) (*ChunkBools, error) {
	nvals := c1.Len()
	bm := bitmap.NewBitmap(nvals)
	for j := 0; j < nvals; j++ {
		if compFn(c1.nthValue(j), c2.nthValue(j)) {
			bm.Set(j, true)
		}
	}
	cdata := newChunkBoolsFromBits(bm.Data(), nvals)
	nulls := bitmap.Or(c1.nullability, c2.nullability)
	if nulls != nil {
		cdata.nullability = nulls
		cdata.nullable = true
	}
	return cdata, nil
}

func compFactoryInts(c1 *ChunkInts, c2 *ChunkInts, compFn func(int64, int64) bool) (*ChunkBools, error) {
	nvals := c1.Len()
	bm := bitmap.NewBitmap(nvals)
	for j, el := range c1.data {
		if compFn(el, c2.data[j]) {
			bm.Set(j, true)
		}
	}
	cdata := newChunkBoolsFromBits(bm.Data(), nvals)
	nulls := bitmap.Or(c1.nullability, c2.nullability)
	if nulls != nil {
		cdata.nullability = nulls
		cdata.nullable = true
	}
	return cdata, nil
}

func compFactoryFloats(c1 *ChunkFloats, c2 *ChunkFloats, compFn func(float64, float64) bool) (*ChunkBools, error) {
	nvals := c1.Len()
	bm := bitmap.NewBitmap(nvals)
	for j, el := range c1.data {
		if compFn(el, c2.data[j]) {
			bm.Set(j, true)
		}
	}
	cdata := newChunkBoolsFromBits(bm.Data(), nvals)
	nulls := bitmap.Or(c1.nullability, c2.nullability)
	if nulls != nil {
		cdata.nullability = nulls
		cdata.nullable = true
	}
	return cdata, nil
}

type compFuncs struct {
	ints    func(int64, int64) bool
	floats  func(float64, float64) bool
	strings func(string, string) bool
}

// OPTIM: what if c1 === c2? short circuit it with a boolean array (copy in the nullability vector though)
// TODO: these evaleq, evalneq, evalgt etc. only differ in the functions passed in - move all of them into one structure
// that generates all of these functions at some point (not at evaluation time)
func algebraicEval(c1 Chunk, c2 Chunk, cf compFuncs) (Chunk, error) {
	if c1.Dtype() != c2.Dtype() {
		// this includes int == float!
		// sort dtypes when implementing this (see the note above)
		return nil, fmt.Errorf("algebraic expression not supported for unequal types: %w", errProjectionNotSupported)
	}

	switch c1.Dtype() {
	case DtypeString:
		return compFactoryStrings(c1.(*ChunkStrings), c2.(*ChunkStrings), cf.strings)
	case DtypeInt:
		return compFactoryInts(c1.(*ChunkInts), c2.(*ChunkInts), cf.ints)
	case DtypeFloat:
		return compFactoryFloats(c1.(*ChunkFloats), c2.(*ChunkFloats), cf.floats)
	default:
		return nil, fmt.Errorf("algebraic expression not supported for types %s and %s: %w", c1.Dtype(), c2.Dtype(), errProjectionNotSupported)
	}
}

func EvalEq(c1 Chunk, c2 Chunk) (Chunk, error) {
	return algebraicEval(c1, c2, compFuncs{
		ints:    func(a, b int64) bool { return a == b },
		floats:  func(a, b float64) bool { return a == b },
		strings: func(a, b string) bool { return a == b },
	})
}

func EvalNeq(c1 Chunk, c2 Chunk) (Chunk, error) {
	return algebraicEval(c1, c2, compFuncs{
		ints:    func(a, b int64) bool { return a != b },
		floats:  func(a, b float64) bool { return a != b },
		strings: func(a, b string) bool { return a != b },
	})
}

func EvalGt(c1 Chunk, c2 Chunk) (Chunk, error) {
	return algebraicEval(c1, c2, compFuncs{
		ints:    func(a, b int64) bool { return a > b },
		floats:  func(a, b float64) bool { return a > b },
		strings: func(a, b string) bool { return a > b },
	})
}

func EvalGte(c1 Chunk, c2 Chunk) (Chunk, error) {
	return algebraicEval(c1, c2, compFuncs{
		ints:    func(a, b int64) bool { return a >= b },
		floats:  func(a, b float64) bool { return a >= b },
		strings: func(a, b string) bool { return a >= b },
	})
}

func EvalLt(c1 Chunk, c2 Chunk) (Chunk, error) {
	return EvalGt(c2, c1)
}

func EvalLte(c1 Chunk, c2 Chunk) (Chunk, error) {
	return EvalGte(c2, c1)
}
