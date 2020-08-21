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

// ARCH: clear unused bits, which we may have altered in compFn? (only relevant for cap%64 != 0)
// we might also have to data ^= ~nullability, so that we clear all the bits that are masked by being nulls
func boolChunkFromParts(data []uint64, length int, null1, null2 *bitmap.Bitmap) *ChunkBools {
	cdata := newChunkBoolsFromBits(data, length)
	nulls := bitmap.Or(null1, null2)
	if nulls != nil {
		cdata.nullability = nulls
	}
	return cdata
}

func compFactoryStrings(c1 *ChunkStrings, c2 *ChunkStrings, compFn func(string, string) bool) (*ChunkBools, error) {
	nvals := c1.Len()
	bm := bitmap.NewBitmap(nvals)
	for j := 0; j < nvals; j++ {
		if compFn(c1.nthValue(j), c2.nthValue(j)) {
			bm.Set(j, true)
		}
	}
	return boolChunkFromParts(bm.Data(), nvals, c1.nullability, c2.nullability), nil
}

func compFactoryInts(c1 *ChunkInts, c2 *ChunkInts, compFn func(int64, int64) bool) (*ChunkBools, error) {
	nvals := c1.Len()
	bm := bitmap.NewBitmap(nvals)
	for j, el := range c1.data {
		if compFn(el, c2.data[j]) {
			bm.Set(j, true)
		}
	}
	return boolChunkFromParts(bm.Data(), nvals, c1.nullability, c2.nullability), nil
}

func compFactoryFloats(c1 *ChunkFloats, c2 *ChunkFloats, compFn func(float64, float64) bool) (*ChunkBools, error) {
	nvals := c1.Len()
	bm := bitmap.NewBitmap(nvals)
	for j, el := range c1.data {
		if compFn(el, c2.data[j]) {
			bm.Set(j, true)
		}
	}
	return boolChunkFromParts(bm.Data(), nvals, c1.nullability, c2.nullability), nil
}

func compFactoryBools(c1 *ChunkBools, c2 *ChunkBools, compFn func(uint64, uint64) uint64) (*ChunkBools, error) {
	nvals := c1.Len()
	c1d := c1.data.Data()
	c2d := c2.data.Data()
	res := make([]uint64, len(c1d))

	for j, el := range c1d {
		res[j] = compFn(el, c2d[j])
	}
	return boolChunkFromParts(res, nvals, c1.nullability, c2.nullability), nil
}

type compFuncs struct {
	ints    func(int64, int64) bool
	floats  func(float64, float64) bool
	strings func(string, string) bool
	bools   func(uint64, uint64) uint64
}

// OPTIM: what if c1 === c2? short circuit it with a boolean array (copy in the nullability vector though)
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
	case DtypeBool:
		return compFactoryBools(c1.(*ChunkBools), c2.(*ChunkBools), cf.bools)
	default:
		return nil, fmt.Errorf("algebraic expression not supported for types %s and %s: %w", c1.Dtype(), c2.Dtype(), errProjectionNotSupported)
	}
}

// EvalAnd produces a bitwise operation on two bool chunks
func EvalAnd(c1 Chunk, c2 Chunk) (Chunk, error) {
	return algebraicEval(c1, c2, compFuncs{
		bools: func(a, b uint64) uint64 { return a & b },
	})
}

// EvalOr produces a bitwise operation on two bool chunks
func EvalOr(c1 Chunk, c2 Chunk) (Chunk, error) {
	return algebraicEval(c1, c2, compFuncs{
		bools: func(a, b uint64) uint64 { return a | b },
	})
}

// EvalEq compares values from two different chunks
func EvalEq(c1 Chunk, c2 Chunk) (Chunk, error) {
	return algebraicEval(c1, c2, compFuncs{
		ints:    func(a, b int64) bool { return a == b },
		floats:  func(a, b float64) bool { return a == b },
		strings: func(a, b string) bool { return a == b },
		bools:   func(a, b uint64) uint64 { return a ^ (^b) },
	})
}

// EvalNeq compares values from two different chunks for inequality
func EvalNeq(c1 Chunk, c2 Chunk) (Chunk, error) {
	return algebraicEval(c1, c2, compFuncs{
		ints:    func(a, b int64) bool { return a != b },
		floats:  func(a, b float64) bool { return a != b },
		strings: func(a, b string) bool { return a != b },
		bools:   func(a, b uint64) uint64 { return a ^ b },
	})
}

// EvalGt checks if values in c1 are greater than in c2
func EvalGt(c1 Chunk, c2 Chunk) (Chunk, error) {
	return algebraicEval(c1, c2, compFuncs{
		ints:    func(a, b int64) bool { return a > b },
		floats:  func(a, b float64) bool { return a > b },
		strings: func(a, b string) bool { return a > b },
		bools:   func(a, b uint64) uint64 { return a & (^b) },
	})
}

// EvalGte checks if values in c1 are greater than or equal to those in c2
func EvalGte(c1 Chunk, c2 Chunk) (Chunk, error) {
	return algebraicEval(c1, c2, compFuncs{
		ints:    func(a, b int64) bool { return a >= b },
		floats:  func(a, b float64) bool { return a >= b },
		strings: func(a, b string) bool { return a >= b },
		bools:   func(a, b uint64) uint64 { return (a & (^b)) | (a ^ (^b)) },
	})
}

// EvalLt checks if values in c1 are lower than in c2
func EvalLt(c1 Chunk, c2 Chunk) (Chunk, error) {
	return EvalGt(c2, c1)
}

// EvalLte checks if values in c1 are lower than or equal to those in c2
func EvalLte(c1 Chunk, c2 Chunk) (Chunk, error) {
	return EvalGte(c2, c1)
}
