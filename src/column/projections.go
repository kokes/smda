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

func boolChunkLiteralFromParts(val bool, length int, null1, null2 *bitmap.Bitmap) *ChunkBools {
	ch := newChunkLiteralBools(val, length)
	nulls := bitmap.Or(null1, null2)
	if nulls != nil {
		ch.nullability = nulls
	}
	return ch
}

func intChunkFromParts(data []int64, null1, null2 *bitmap.Bitmap) *ChunkInts {
	nulls := bitmap.Or(null1, null2)
	return newChunkIntsFromSlice(data, nulls)
}
func floatChunkFromParts(data []float64, null1, null2 *bitmap.Bitmap) *ChunkFloats {
	nulls := bitmap.Or(null1, null2)
	return newChunkFloatsFromSlice(data, nulls)
}

func compFactoryStrings(c1 *ChunkStrings, c2 *ChunkStrings, compFn func(string, string) bool) (*ChunkBools, error) {
	nvals := c1.Len()
	if c1.isLiteral && c2.isLiteral {
		// OPTIM: this should be a part of constant folding and should never get to this point
		val := compFn(c1.nthValue(0), c2.nthValue(0))
		return boolChunkLiteralFromParts(val, nvals, c1.nullability, c2.nullability), nil
	}

	bm := bitmap.NewBitmap(nvals)
	eval := func(j int) bool { return compFn(c1.nthValue(j), c2.nthValue(j)) }
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
	for j := 0; j < nvals; j++ {
		if eval(j) {
			bm.Set(j, true)
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

	if c1.isLiteral && c2.isLiteral {
		// OPTIM: this should be a part of constant folding and should never get to this point
		val := compFn(c1.data[0], c2.data[0])
		return boolChunkLiteralFromParts(val, nvals, c1.nullability, c2.nullability), nil
	}

	bm := bitmap.NewBitmap(nvals)
	eval := func(j int) bool { return compFn(c1.data[j], c2.data[j]) }
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
	return boolChunkFromParts(bm.Data(), nvals, c1.nullability, c2.nullability), nil
}

// ARCH: this function is identical to compFactoryInts, so it's probably the first to make use of generics
func compFactoryFloats(c1 *ChunkFloats, c2 *ChunkFloats, compFn func(float64, float64) bool) (*ChunkBools, error) {
	nvals := c1.Len()
	bm := bitmap.NewBitmap(nvals)

	if c1.isLiteral && c2.isLiteral {
		// OPTIM: this should be a part of constant folding and should never get to this point
		val := compFn(c1.data[0], c2.data[0])
		return boolChunkLiteralFromParts(val, nvals, c1.nullability, c2.nullability), nil

	}

	eval := func(j int) bool { return compFn(c1.data[j], c2.data[j]) }
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
	return boolChunkFromParts(bm.Data(), nvals, c1.nullability, c2.nullability), nil
}

// ARCH: this is, again, identical to the previous factory functions
func compFactoryIntsFloats(c1 *ChunkInts, c2 *ChunkFloats, compFn func(int64, float64) bool) (*ChunkBools, error) {
	nvals := c1.Len()
	bm := bitmap.NewBitmap(nvals)

	if c1.isLiteral && c2.isLiteral {
		// OPTIM: this should be a part of constant folding and should never get to this point
		val := compFn(c1.data[0], c2.data[0])
		return boolChunkLiteralFromParts(val, nvals, c1.nullability, c2.nullability), nil

	}

	eval := func(j int) bool { return compFn(c1.data[j], c2.data[j]) }
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
	return boolChunkFromParts(bm.Data(), nvals, c1.nullability, c2.nullability), nil
}

// ARCH: this is, again, identical to the previous factory functions
func compFactoryFloatsInts(c1 *ChunkFloats, c2 *ChunkInts, compFn func(float64, int64) bool) (*ChunkBools, error) {
	nvals := c1.Len()
	bm := bitmap.NewBitmap(nvals)

	if c1.isLiteral && c2.isLiteral {
		// OPTIM: this should be a part of constant folding and should never get to this point
		val := compFn(c1.data[0], c2.data[0])
		return boolChunkLiteralFromParts(val, nvals, c1.nullability, c2.nullability), nil

	}

	eval := func(j int) bool { return compFn(c1.data[j], c2.data[j]) }
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
	return boolChunkFromParts(bm.Data(), nvals, c1.nullability, c2.nullability), nil
}

const ALL_ZEROS = uint64(0)
const ALL_ONES = uint64(1<<64 - 1)

func compFactoryBools(c1 *ChunkBools, c2 *ChunkBools, compFn func(uint64, uint64) uint64) (*ChunkBools, error) {
	nvals := c1.Len()
	res := make([]uint64, (nvals+63)/64)

	if c1.isLiteral && c2.isLiteral {
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
	}

	c1d := c1.data.Data()
	c2d := c2.data.Data()
	eval := func(j int) uint64 { return compFn(c1d[j], c2d[j]) }
	if c1.isLiteral {
		mask := ALL_ZEROS
		if c1.data.Get(0) {
			mask = ALL_ONES
		}
		eval = func(j int) uint64 { return compFn(mask, c2d[j]) }
	}
	if c2.isLiteral {
		mask := ALL_ZEROS
		if c2.data.Get(0) {
			mask = ALL_ONES
		}
		eval = func(j int) uint64 { return compFn(c1d[j], mask) }
	}
	for j := 0; j < len(res); j++ {
		res[j] = eval(j)
	}

	// we may have flipped some bits that are not relevant (beyond the bitmap's cap)
	// so we have to reset them
	// ARCH: technically we don't have to, there's no contract regarding masked bits
	//       and inaccessible bits
	if nvals%64 != 0 {
		rem := nvals % 64
		mask := uint64(1<<rem - 1)
		res[len(res)-1] &= mask
	}

	return boolChunkFromParts(res, nvals, c1.nullability, c2.nullability), nil
}

type compFuncs struct {
	ints     func(int64, int64) bool
	floats   func(float64, float64) bool
	intfloat func(int64, float64) bool
	floatint func(float64, int64) bool
	strings  func(string, string) bool
	bools    func(uint64, uint64) uint64
}

// OPTIM: what if c1 === c2? short circuit it with a boolean array (copy in the nullability vector though)
func compEval(c1 Chunk, c2 Chunk, cf compFuncs) (Chunk, error) {
	if c1.Dtype() == c2.Dtype() {
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
			return nil, fmt.Errorf("comparison expression not supported for types %s and %s: %w", c1.Dtype(), c2.Dtype(), errProjectionNotSupported)
		}
	}

	type dtypes struct{ a, b Dtype }
	c1d := c1.Dtype()
	c2d := c2.Dtype()
	cs := dtypes{c1d, c2d}
	switch cs {
	case dtypes{DtypeInt, DtypeFloat}:
		return compFactoryIntsFloats(c1.(*ChunkInts), c2.(*ChunkFloats), cf.intfloat)
	case dtypes{DtypeFloat, DtypeInt}:
		return compFactoryFloatsInts(c1.(*ChunkFloats), c2.(*ChunkInts), cf.floatint)
	default:
		return nil, fmt.Errorf("comparison expression not supported for types %v and %v: %w", c1d, c2d, errProjectionNotSupported)

	}
}

// EvalAnd produces a bitwise operation on two bool chunks
func EvalAnd(c1 Chunk, c2 Chunk) (Chunk, error) {
	return compEval(c1, c2, compFuncs{
		bools: func(a, b uint64) uint64 { return a & b },
	})
}

// EvalOr produces a bitwise operation on two bool chunks
func EvalOr(c1 Chunk, c2 Chunk) (Chunk, error) {
	return compEval(c1, c2, compFuncs{
		bools: func(a, b uint64) uint64 { return a | b },
	})
}

// EvalEq compares values from two different chunks
func EvalEq(c1 Chunk, c2 Chunk) (Chunk, error) {
	return compEval(c1, c2, compFuncs{
		ints:     func(a, b int64) bool { return a == b },
		floats:   func(a, b float64) bool { return a == b },
		intfloat: func(a int64, b float64) bool { return float64(a) == b },
		floatint: func(a float64, b int64) bool { return a == float64(b) },
		strings:  func(a, b string) bool { return a == b },
		bools:    func(a, b uint64) uint64 { return a ^ (^b) },
	})
}

// EvalNeq compares values from two different chunks for inequality
func EvalNeq(c1 Chunk, c2 Chunk) (Chunk, error) {
	return compEval(c1, c2, compFuncs{
		ints:     func(a, b int64) bool { return a != b },
		floats:   func(a, b float64) bool { return a != b },
		intfloat: func(a int64, b float64) bool { return float64(a) != b },
		floatint: func(a float64, b int64) bool { return a != float64(b) },
		strings:  func(a, b string) bool { return a != b },
		bools:    func(a, b uint64) uint64 { return a ^ b },
	})
}

// EvalGt checks if values in c1 are greater than in c2
func EvalGt(c1 Chunk, c2 Chunk) (Chunk, error) {
	return compEval(c1, c2, compFuncs{
		ints:     func(a, b int64) bool { return a > b },
		floats:   func(a, b float64) bool { return a > b },
		intfloat: func(a int64, b float64) bool { return float64(a) > b },
		floatint: func(a float64, b int64) bool { return a > float64(b) },
		strings:  func(a, b string) bool { return a > b },
		bools:    func(a, b uint64) uint64 { return a & (^b) },
	})
}

// EvalGte checks if values in c1 are greater than or equal to those in c2
func EvalGte(c1 Chunk, c2 Chunk) (Chunk, error) {
	return compEval(c1, c2, compFuncs{
		ints:     func(a, b int64) bool { return a >= b },
		floats:   func(a, b float64) bool { return a >= b },
		intfloat: func(a int64, b float64) bool { return float64(a) >= b },
		floatint: func(a float64, b int64) bool { return a >= float64(b) },
		strings:  func(a, b string) bool { return a >= b },
		bools:    func(a, b uint64) uint64 { return (a & (^b)) | (a ^ (^b)) },
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

// ARCH: either get rid of all this via generic, or, better yet, rewrite all the algebraics
// using functions. We could then, like in Julia (or lisps), have a function -(a, b)
type algebraFuncs struct {
	ints func(int64, int64) int64
	// so far just for division, ARCH: cast and use intfloat instead?
	intsf    func(int64, int64) float64
	floats   func(float64, float64) float64
	intfloat func(int64, float64) float64
	floatint func(float64, int64) float64
}

func algebraFactoryInts(c1 *ChunkInts, c2 *ChunkInts, compFn func(int64, int64) int64) (*ChunkInts, error) {
	nvals := c1.Len()

	if c1.isLiteral && c2.isLiteral {
		// OPTIM: this should be a part of constant folding and should never get to this point
		val := compFn(c1.data[0], c2.data[0])
		return newChunkLiteralInts(val, nvals), nil
	}
	var eval func(j int) int64
	eval = func(j int) int64 { return compFn(c1.data[j], c2.data[j]) }
	if c1.isLiteral {
		val := c1.data[0]
		eval = func(j int) int64 { return compFn(val, c2.data[j]) }
	}
	if c2.isLiteral {
		val := c2.data[0]
		eval = func(j int) int64 { return compFn(c1.data[j], val) }
	}
	ret := make([]int64, nvals)
	for j := 0; j < nvals; j++ {
		ret[j] = eval(j)
	}
	return intChunkFromParts(ret, c1.nullability, c2.nullability), nil
}

// ARCH: this is identical to `algebraFactoryFloats` apart from the compFn signature in the argument
func algebraFactoryIntsf(c1 *ChunkInts, c2 *ChunkInts, compFn func(int64, int64) float64) (*ChunkFloats, error) {
	nvals := c1.Len()

	if c1.isLiteral && c2.isLiteral {
		// OPTIM: this should be a part of constant folding and should never get to this point
		val := compFn(c1.data[0], c2.data[0])
		return newChunkLiteralFloats(val, nvals), nil
	}
	var eval func(j int) float64
	eval = func(j int) float64 { return compFn(c1.data[j], c2.data[j]) }
	if c1.isLiteral {
		val := c1.data[0]
		eval = func(j int) float64 { return compFn(val, c2.data[j]) }
	}
	if c2.isLiteral {
		val := c2.data[0]
		eval = func(j int) float64 { return compFn(c1.data[j], val) }
	}
	ret := make([]float64, nvals)
	for j := 0; j < nvals; j++ {
		ret[j] = eval(j)
	}
	return floatChunkFromParts(ret, c1.nullability, c2.nullability), nil
}

func algebraFactoryFloats(c1 *ChunkFloats, c2 *ChunkFloats, compFn func(float64, float64) float64) (*ChunkFloats, error) {
	nvals := c1.Len()

	if c1.isLiteral && c2.isLiteral {
		// OPTIM: this should be a part of constant folding and should never get to this point
		val := compFn(c1.data[0], c2.data[0])
		return newChunkLiteralFloats(val, nvals), nil
	}
	var eval func(j int) float64
	eval = func(j int) float64 { return compFn(c1.data[j], c2.data[j]) }
	if c1.isLiteral {
		val := c1.data[0]
		eval = func(j int) float64 { return compFn(val, c2.data[j]) }
	}
	if c2.isLiteral {
		val := c2.data[0]
		eval = func(j int) float64 { return compFn(c1.data[j], val) }
	}
	ret := make([]float64, nvals)
	for j := 0; j < nvals; j++ {
		ret[j] = eval(j)
	}
	return floatChunkFromParts(ret, c1.nullability, c2.nullability), nil
}

// ARCH: this is identical to `algebraFactoryFloats` apart from the compFn signature in the argument
func algebraFactoryIntFloat(c1 *ChunkInts, c2 *ChunkFloats, compFn func(int64, float64) float64) (*ChunkFloats, error) {
	nvals := c1.Len()

	if c1.isLiteral && c2.isLiteral {
		// OPTIM: this should be a part of constant folding and should never get to this point
		val := compFn(c1.data[0], c2.data[0])
		return newChunkLiteralFloats(val, nvals), nil
	}
	var eval func(j int) float64
	eval = func(j int) float64 { return compFn(c1.data[j], c2.data[j]) }
	if c1.isLiteral {
		val := c1.data[0]
		eval = func(j int) float64 { return compFn(val, c2.data[j]) }
	}
	if c2.isLiteral {
		val := c2.data[0]
		eval = func(j int) float64 { return compFn(c1.data[j], val) }
	}
	ret := make([]float64, nvals)
	for j := 0; j < nvals; j++ {
		ret[j] = eval(j)
	}
	return floatChunkFromParts(ret, c1.nullability, c2.nullability), nil
}

// ARCH: this is identical to `algebraFactoryFloats` apart from the compFn signature in the argument
func algebraFactoryFloatInt(c1 *ChunkFloats, c2 *ChunkInts, compFn func(float64, int64) float64) (*ChunkFloats, error) {
	nvals := c1.Len()

	if c1.isLiteral && c2.isLiteral {
		// OPTIM: this should be a part of constant folding and should never get to this point
		val := compFn(c1.data[0], c2.data[0])
		return newChunkLiteralFloats(val, nvals), nil
	}
	var eval func(j int) float64
	eval = func(j int) float64 { return compFn(c1.data[j], c2.data[j]) }
	if c1.isLiteral {
		val := c1.data[0]
		eval = func(j int) float64 { return compFn(val, c2.data[j]) }
	}
	if c2.isLiteral {
		val := c2.data[0]
		eval = func(j int) float64 { return compFn(c1.data[j], val) }
	}
	ret := make([]float64, nvals)
	for j := 0; j < nvals; j++ {
		ret[j] = eval(j)
	}
	return floatChunkFromParts(ret, c1.nullability, c2.nullability), nil
}

func algebraicEval(c1 Chunk, c2 Chunk, commutative bool, cf algebraFuncs) (Chunk, error) {
	errg := func(c1d, c2d Dtype) error {
		return fmt.Errorf("algebraic expression not supported for types %s and %s: %w", c1d, c2d, errProjectionNotSupported)
	}
	c1d := c1.Dtype()
	c2d := c2.Dtype()
	if c1d == c2d {
		switch c1d {
		case DtypeInt:
			// if intsf exists, dispatch that instead (for division)
			if cf.intsf != nil {
				// TODO: check if cf.ints exists and panic?
				return algebraFactoryIntsf(c1.(*ChunkInts), c2.(*ChunkInts), cf.intsf)
			}
			return algebraFactoryInts(c1.(*ChunkInts), c2.(*ChunkInts), cf.ints)
		case DtypeFloat:
			return algebraFactoryFloats(c1.(*ChunkFloats), c2.(*ChunkFloats), cf.floats)
		default:
			return nil, errg(c1d, c2d)
		}
	}

	type dtypes struct{ a, b Dtype }
	cs := dtypes{c1d, c2d}
	switch cs {
	case dtypes{DtypeInt, DtypeFloat}:
		if commutative && cf.intfloat == nil {
			// could have returned `algebraicEval` instead, which would be more future proof
			// but also way less performant (will change in case we implement static dispatch)
			return algebraFactoryFloatInt(c2.(*ChunkFloats), c1.(*ChunkInts), cf.floatint)
		}
		return algebraFactoryIntFloat(c1.(*ChunkInts), c2.(*ChunkFloats), cf.intfloat)
	case dtypes{DtypeFloat, DtypeInt}:
		if commutative && cf.floatint == nil {
			return algebraFactoryIntFloat(c2.(*ChunkInts), c1.(*ChunkFloats), cf.intfloat)
		}
		return algebraFactoryFloatInt(c1.(*ChunkFloats), c2.(*ChunkInts), cf.floatint)
	default:
		return nil, errg(c1d, c2d)
	}
}

// a solid case for generics?
func EvalAdd(c1 Chunk, c2 Chunk) (Chunk, error) {
	return algebraicEval(c1, c2, true, algebraFuncs{
		ints:     func(a, b int64) int64 { return a + b },
		floats:   func(a, b float64) float64 { return a + b },
		intfloat: func(a int64, b float64) float64 { return float64(a) + b }, // commutative
	})
}

func EvalSubtract(c1 Chunk, c2 Chunk) (Chunk, error) {
	return algebraicEval(c1, c2, false, algebraFuncs{
		ints:     func(a, b int64) int64 { return a - b },
		floats:   func(a, b float64) float64 { return a - b },
		intfloat: func(a int64, b float64) float64 { return float64(a) - b }, // commutative only with a multiplication
		floatint: func(a float64, b int64) float64 { return a - float64(b) },
	})
}

// different return type for ints! should we perhaps cast to make this more systematic?
// check for division by zero (gives +- infty, which will break json?)
func EvalDivide(c1 Chunk, c2 Chunk) (Chunk, error) {
	return algebraicEval(c1, c2, false, algebraFuncs{
		intsf:    func(a, b int64) float64 { return float64(a) / float64(b) },
		floats:   func(a, b float64) float64 { return a / b },
		intfloat: func(a int64, b float64) float64 { return float64(a) / b }, // not commutative
		floatint: func(a float64, b int64) float64 { return a / float64(b) },
	})
}

func EvalMultiply(c1 Chunk, c2 Chunk) (Chunk, error) {
	return algebraicEval(c1, c2, true, algebraFuncs{
		ints:     func(a, b int64) int64 { return a * b },
		floats:   func(a, b float64) float64 { return a * b },
		intfloat: func(a int64, b float64) float64 { return float64(a) * b }, // commutative
	})
}
