package column

import (
	"errors"
	"fmt"

	"github.com/kokes/smda/src/bitmap"
)

// TODO: rename to comparisons.go?

var errProjectionNotSupported = errors.New("projection not supported")

// one thing that might help us with all the implementations of functions with 2+ arguments:
// sort them by dtypes (if possible!), that way we can implement far fewer cases
// in some cases (e.g. equality), we can simply swap the arguments
// in other cases (e.g. greater than), we need to swap the operator as well

// also, where will we deal with nulls? this should be in as few places as possible

// when this gets too long, split it up into projections_string, projections_date etc.

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
	// if one column is a literal, it won't have the right length set
	// TODO: remove this once we implement stripe lengths
	if c2.Len() > nvals {
		nvals = c2.Len()
	}
	bm := bitmap.NewBitmap(nvals)
	switch {
	case c1.isLiteral && c2.isLiteral:
		// OPTIM: this should be a part of constant folding and should never get to this point
		val := compFn(c1.nthValue(0), c2.nthValue(0))
		if val {
			bm.Invert()
		}
	case c1.isLiteral || c2.isLiteral:
		var eval func(j int) bool
		if c1.isLiteral {
			val := c1.nthValue(0)
			eval = func(j int) bool { return compFn(val, c2.nthValue(j)) }
		}
		if c2.isLiteral {
			val := c2.nthValue(0)
			eval = func(j int) bool { return compFn(c1.nthValue(j), val) }
		}
		for j := 0; j < nvals; j++ {
			if eval(j) {
				bm.Set(j, true)
			}
		}
	default:
		for j := 0; j < nvals; j++ {
			if compFn(c1.nthValue(j), c2.nthValue(j)) {
				bm.Set(j, true)
			}
		}
	}

	return boolChunkFromParts(bm.Data(), nvals, c1.nullability, c2.nullability), nil
}

// OPTIM: instead of treating literals in a separate tree, we could have data access functions:
//		`c1data(j) = func(...) {return c1.Data[j]}` for dense chunks and
//		`c1data(j) = func(...) return c1.Data[0]}` for literals
// I'm worried that this runtime func assignment will limit inlining and thus lead to large overhead of
// function calls
// Maybe try this once we have tests and benchmarks in place
func compFactoryInts(c1 *ChunkInts, c2 *ChunkInts, compFn func(int64, int64) bool) (*ChunkBools, error) {
	nvals := c1.Len()
	// if one column is a literal, it won't have the right length set
	// TODO: remove this once we implement stripe lengths
	if c2.Len() > nvals {
		nvals = c2.Len()
	}
	bm := bitmap.NewBitmap(nvals)

	switch {
	case c1.isLiteral && c2.isLiteral:
		// OPTIM: this should be a part of constant folding and should never get to this point
		val := compFn(c1.data[0], c2.data[0])
		if val {
			bm.Invert()
		}
	case c1.isLiteral || c2.isLiteral:
		var eval func(j int) bool
		if c1.isLiteral {
			val := c1.data[0]
			eval = func(j int) bool { return compFn(val, c2.data[j]) }
		}
		if c2.isLiteral {
			val := c2.data[0]
			eval = func(j int) bool { return compFn(c1.data[j], val) }
		}
		for j := 0; j < nvals; j++ {
			if eval(j) {
				bm.Set(j, true)
			}
		}
	default:
		for j, el := range c1.data {
			if compFn(el, c2.data[j]) {
				bm.Set(j, true)
			}
		}
	}
	return boolChunkFromParts(bm.Data(), nvals, c1.nullability, c2.nullability), nil
}

// ARCH: this function is identical to compFactoryInts, so it's probably the first to make use of generics
func compFactoryFloats(c1 *ChunkFloats, c2 *ChunkFloats, compFn func(float64, float64) bool) (*ChunkBools, error) {
	nvals := c1.Len()
	// if one column is a literal, it won't have the right length set
	// TODO: remove this once we implement stripe lengths
	if c2.Len() > nvals {
		nvals = c2.Len()
	}
	bm := bitmap.NewBitmap(nvals)

	switch {
	case c1.isLiteral && c2.isLiteral:
		// OPTIM: this should be a part of constant folding and should never get to this point
		val := compFn(c1.data[0], c2.data[0])
		if val {
			bm.Invert()
		}
	case c1.isLiteral || c2.isLiteral:
		var eval func(j int) bool
		if c1.isLiteral {
			val := c1.data[0]
			eval = func(j int) bool { return compFn(val, c2.data[j]) }
		}
		if c2.isLiteral {
			val := c2.data[0]
			eval = func(j int) bool { return compFn(c1.data[j], val) }
		}
		for j := 0; j < nvals; j++ {
			if eval(j) {
				bm.Set(j, true)
			}
		}
	default:
		for j, el := range c1.data {
			if compFn(el, c2.data[j]) {
				bm.Set(j, true)
			}
		}
	}
	return boolChunkFromParts(bm.Data(), nvals, c1.nullability, c2.nullability), nil
}

const ALL_ZEROS = uint64(0)
const ALL_ONES = uint64(1<<64 - 1)

func compFactoryBools(c1 *ChunkBools, c2 *ChunkBools, compFn func(uint64, uint64) uint64) (*ChunkBools, error) {
	nvals := c1.Len()
	// if one column is a literal, it won't have the right length set
	// TODO: remove this once we implement stripe lengths
	if c2.Len() > nvals {
		nvals = c2.Len()
	}
	res := make([]uint64, (nvals+63)/64)

	switch {
	case c1.isLiteral && c2.isLiteral:
		// OPTIM: this should be a part of constant folding and should never get to this point
		panic("not implemented yet") // TODO
		// idea: compFn the thing, extract that one relevant bit and then set all values in res
		// to either all zeroes or all ones
		// but first get some test cases for it (be careful about all the other bits in val)
		// val := compFn(c1.data.Data()[0], c2.data.Data()[0])
		// if val & 1 > 0 {
		// 	for j := 0; j < len(res); j++ {
		// 		res[j] = ALL_ONES
		// 	}
		// }
	case c1.isLiteral || c2.isLiteral:
		var eval func(j int) uint64
		if c1.isLiteral {
			mask := ALL_ZEROS
			if c1.data.Get(0) {
				mask = ALL_ONES
			}
			data := c2.data.Data()
			eval = func(j int) uint64 { return compFn(mask, data[j]) }
		}
		if c2.isLiteral {
			mask := ALL_ZEROS
			if c2.data.Get(0) {
				mask = ALL_ONES
			}
			data := c1.data.Data()
			eval = func(j int) uint64 { return compFn(data[j], mask) }
		}
		for j := 0; j < len(res); j++ {
			res[j] = eval(j)
		}
	default:
		c2d := c2.data.Data()
		for j, el := range c1.data.Data() {
			res[j] = compFn(el, c2d[j])
		}
	}

	// we may have flipped some bits that are not relevant (beyond the bitmap's cap)
	// so we have to reset them
	if nvals%64 != 0 {
		rem := nvals % 64
		mask := uint64(1<<rem - 1)
		res[len(res)-1] &= mask
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
