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

func boolChunkFromParts(data []uint64, length int, null1, null2 *bitmap.Bitmap) *Chunk {
	cdata := newChunkBoolsFromBits(data, length)
	nulls := bitmap.Or(null1, null2)
	if nulls != nil {
		cdata.Nullability = nulls
	}
	return cdata
}

func boolChunkLiteralFromParts(val bool, length int, null1, null2 *bitmap.Bitmap) *Chunk {
	ch := NewChunkLiteralBools(val, length)
	nulls := bitmap.Or(null1, null2)
	if nulls != nil {
		ch.Nullability = nulls
	}
	return ch
}

func intChunkFromParts(data []int64, null1, null2 *bitmap.Bitmap) *Chunk {
	nulls := bitmap.Or(null1, null2)
	return NewChunkIntsFromSlice(data, nulls)
}
func floatChunkFromParts(data []float64, null1, null2 *bitmap.Bitmap) *Chunk {
	nulls := bitmap.Or(null1, null2)
	return NewChunkFloatsFromSlice(data, nulls)
}

func EvalNot(c *Chunk) (*Chunk, error) {
	if c.dtype != DtypeBool {
		return nil, fmt.Errorf("%w: cannot evaluate NOT on non-bool columns (%v)", errProjectionNotSupported, c.dtype)
	}
	ret := c.Clone()
	ret.storage.bools.Invert()
	return ret, nil
}

func compFactoryStrings(c1 *Chunk, c2 *Chunk, compFn func(string, string) bool) (*Chunk, error) {
	nvals := c1.Len()
	if c1.IsLiteral && c2.IsLiteral {
		// OPTIM: this should be a part of constant folding and should never get to this point
		val := compFn(c1.nthValue(0), c2.nthValue(0))
		return boolChunkLiteralFromParts(val, nvals, c1.Nullability, c2.Nullability), nil
	}

	bm := bitmap.NewBitmap(nvals)
	eval := func(j int) bool { return compFn(c1.nthValue(j), c2.nthValue(j)) }
	if c1.IsLiteral {
		val := c1.nthValue(0)
		eval = func(j int) bool { return compFn(val, c2.nthValue(j)) }
	}
	if c2.IsLiteral {
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

	return boolChunkFromParts(bm.Data(), nvals, c1.Nullability, c2.Nullability), nil
}

// OPTIM: instead of treating literals in a separate tree, we could have data access functions:
//		`c1data(j) = func(...) {return c1.Data[j]}` for dense chunks and
//		`c1data(j) = func(...) return c1.Data[0]}` for literals
// I'm worried that this runtime func assignment will limit inlining and thus lead to large overhead of
// function calls
// Maybe try this once we have tests and benchmarks in place
func compFactoryInts(c1 *Chunk, c2 *Chunk, compFn func(int64, int64) bool) (*Chunk, error) {
	nvals := c1.Len()

	if c1.IsLiteral && c2.IsLiteral {
		// OPTIM: this should be a part of constant folding and should never get to this point
		val := compFn(c1.storage.ints[0], c2.storage.ints[0])
		return boolChunkLiteralFromParts(val, nvals, c1.Nullability, c2.Nullability), nil
	}

	bm := bitmap.NewBitmap(nvals)
	eval := func(j int) bool { return compFn(c1.storage.ints[j], c2.storage.ints[j]) }
	if c1.IsLiteral {
		val := c1.storage.ints[0]
		eval = func(j int) bool { return compFn(val, c2.storage.ints[j]) }
	}
	if c2.IsLiteral {
		val := c2.storage.ints[0]
		eval = func(j int) bool { return compFn(c1.storage.ints[j], val) }
	}
	for j := 0; j < nvals; j++ {
		// OPTIM: cannot inline re-assigned closure variable at src/column/projections.go:99:8: eval = func literal
		if eval(j) {
			bm.Set(j, true)
		}
	}
	return boolChunkFromParts(bm.Data(), nvals, c1.Nullability, c2.Nullability), nil
}

// ARCH: this function is identical to compFactoryInts, so it's probably the first to make use of generics
func compFactoryFloats(c1 *Chunk, c2 *Chunk, compFn func(float64, float64) bool) (*Chunk, error) {
	nvals := c1.Len()
	bm := bitmap.NewBitmap(nvals)

	if c1.IsLiteral && c2.IsLiteral {
		// OPTIM: this should be a part of constant folding and should never get to this point
		val := compFn(c1.storage.floats[0], c2.storage.floats[0])
		return boolChunkLiteralFromParts(val, nvals, c1.Nullability, c2.Nullability), nil

	}

	eval := func(j int) bool { return compFn(c1.storage.floats[j], c2.storage.floats[j]) }
	if c1.IsLiteral {
		val := c1.storage.floats[0]
		eval = func(j int) bool { return compFn(val, c2.storage.floats[j]) }
	}
	if c2.IsLiteral {
		val := c2.storage.floats[0]
		eval = func(j int) bool { return compFn(c1.storage.floats[j], val) }
	}

	for j := 0; j < nvals; j++ {
		if eval(j) {
			bm.Set(j, true)
		}

	}
	return boolChunkFromParts(bm.Data(), nvals, c1.Nullability, c2.Nullability), nil
}

// ARCH: this is, again, identical to the previous factory functions
func compFactoryIntsFloats(c1 *Chunk, c2 *Chunk, compFn func(int64, float64) bool) (*Chunk, error) {
	nvals := c1.Len()
	bm := bitmap.NewBitmap(nvals)

	if c1.IsLiteral && c2.IsLiteral {
		// OPTIM: this should be a part of constant folding and should never get to this point
		val := compFn(c1.storage.ints[0], c2.storage.floats[0])
		return boolChunkLiteralFromParts(val, nvals, c1.Nullability, c2.Nullability), nil

	}

	eval := func(j int) bool { return compFn(c1.storage.ints[j], c2.storage.floats[j]) }
	if c1.IsLiteral {
		val := c1.storage.ints[0]
		eval = func(j int) bool { return compFn(val, c2.storage.floats[j]) }
	}
	if c2.IsLiteral {
		val := c2.storage.floats[0]
		eval = func(j int) bool { return compFn(c1.storage.ints[j], val) }
	}

	for j := 0; j < nvals; j++ {
		if eval(j) {
			bm.Set(j, true)
		}

	}
	return boolChunkFromParts(bm.Data(), nvals, c1.Nullability, c2.Nullability), nil
}

// ARCH: this is, again, identical to the previous factory functions
func compFactoryFloatsInts(c1 *Chunk, c2 *Chunk, compFn func(float64, int64) bool) (*Chunk, error) {
	nvals := c1.Len()
	bm := bitmap.NewBitmap(nvals)

	if c1.IsLiteral && c2.IsLiteral {
		// OPTIM: this should be a part of constant folding and should never get to this point
		val := compFn(c1.storage.floats[0], c2.storage.ints[0])
		return boolChunkLiteralFromParts(val, nvals, c1.Nullability, c2.Nullability), nil

	}

	eval := func(j int) bool { return compFn(c1.storage.floats[j], c2.storage.ints[j]) }
	if c1.IsLiteral {
		val := c1.storage.floats[0]
		eval = func(j int) bool { return compFn(val, c2.storage.ints[j]) }
	}
	if c2.IsLiteral {
		val := c2.storage.ints[0]
		eval = func(j int) bool { return compFn(c1.storage.floats[j], val) }
	}

	for j := 0; j < nvals; j++ {
		if eval(j) {
			bm.Set(j, true)
		}

	}
	return boolChunkFromParts(bm.Data(), nvals, c1.Nullability, c2.Nullability), nil
}

const ALL_ZEROS = uint64(0)
const ALL_ONES = uint64(1<<64 - 1)

func compFactoryBools(c1 *Chunk, c2 *Chunk, compFn func(uint64, uint64) uint64) (*Chunk, error) {
	nvals := c1.Len()

	if c1.IsLiteral && c2.IsLiteral {
		// OPTIM: this should be a part of constant folding and should never get to this point
		val := compFn(c1.storage.bools.Data()[0], c2.storage.bools.Data()[0])
		return NewChunkLiteralBools(val&1 > 0, nvals), nil // TODO: should this be `boolChunkLiteralFromParts`?
	}
	res := make([]uint64, (nvals+63)/64)

	c1d := c1.storage.bools.Data()
	c2d := c2.storage.bools.Data()
	eval := func(j int) uint64 { return compFn(c1d[j], c2d[j]) }
	if c1.IsLiteral {
		mask := ALL_ZEROS
		if c1.storage.bools.Get(0) {
			mask = ALL_ONES
		}
		eval = func(j int) uint64 { return compFn(mask, c2d[j]) }
	}
	if c2.IsLiteral {
		mask := ALL_ZEROS
		if c2.storage.bools.Get(0) {
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

	return boolChunkFromParts(res, nvals, c1.Nullability, c2.Nullability), nil
}

// ARCH: many if not all of these could be implemented in generics
func compFactoryDates(c1 *Chunk, c2 *Chunk, compFn func(date, date) bool) (*Chunk, error) {
	nvals := c1.Len()

	if c1.IsLiteral && c2.IsLiteral {
		// OPTIM: this should be a part of constant folding and should never get to this point
		val := compFn(c1.storage.dates[0], c2.storage.dates[0])
		return boolChunkLiteralFromParts(val, nvals, c1.Nullability, c2.Nullability), nil
	}

	bm := bitmap.NewBitmap(nvals)
	eval := func(j int) bool { return compFn(c1.storage.dates[j], c2.storage.dates[j]) }
	if c1.IsLiteral {
		val := c1.storage.dates[0]
		eval = func(j int) bool { return compFn(val, c2.storage.dates[j]) }
	}
	if c2.IsLiteral {
		val := c2.storage.dates[0]
		eval = func(j int) bool { return compFn(c1.storage.dates[j], val) }
	}
	for j := 0; j < nvals; j++ {
		// OPTIM: cannot inline re-assigned closure variable at src/column/projections.go:99:8: eval = func literal
		if eval(j) {
			bm.Set(j, true)
		}
	}
	return boolChunkFromParts(bm.Data(), nvals, c1.Nullability, c2.Nullability), nil
}

func compFactoryDatetimes(c1 *Chunk, c2 *Chunk, compFn func(datetime, datetime) bool) (*Chunk, error) {
	nvals := c1.Len()

	if c1.IsLiteral && c2.IsLiteral {
		// OPTIM: this should be a part of constant folding and should never get to this point
		val := compFn(c1.storage.datetimes[0], c2.storage.datetimes[0])
		return boolChunkLiteralFromParts(val, nvals, c1.Nullability, c2.Nullability), nil
	}

	bm := bitmap.NewBitmap(nvals)
	eval := func(j int) bool { return compFn(c1.storage.datetimes[j], c2.storage.datetimes[j]) }
	if c1.IsLiteral {
		val := c1.storage.datetimes[0]
		eval = func(j int) bool { return compFn(val, c2.storage.datetimes[j]) }
	}
	if c2.IsLiteral {
		val := c2.storage.datetimes[0]
		eval = func(j int) bool { return compFn(c1.storage.datetimes[j], val) }
	}
	for j := 0; j < nvals; j++ {
		// OPTIM: cannot inline re-assigned closure variable at src/column/projections.go:99:8: eval = func literal
		if eval(j) {
			bm.Set(j, true)
		}
	}
	return boolChunkFromParts(bm.Data(), nvals, c1.Nullability, c2.Nullability), nil
}

type compFuncs struct {
	ints      func(int64, int64) bool
	floats    func(float64, float64) bool
	intfloat  func(int64, float64) bool
	floatint  func(float64, int64) bool
	strings   func(string, string) bool
	bools     func(uint64, uint64) uint64
	dates     func(date, date) bool
	datetimes func(datetime, datetime) bool
}

// OPTIM: what if c1 === c2? short circuit it with a boolean array (copy in the nullability vector though)
func compEval(c1 *Chunk, c2 *Chunk, cf compFuncs) (*Chunk, error) {
	err := fmt.Errorf("comparison expression not supported for types %s and %s: %w", c1.dtype, c2.dtype, errProjectionNotSupported)
	if c1.dtype == c2.dtype {
		switch c1.dtype {
		case DtypeString:
			if cf.strings == nil {
				return nil, err
			}
			return compFactoryStrings(c1, c2, cf.strings)
		case DtypeInt:
			if cf.ints == nil {
				return nil, err
			}
			return compFactoryInts(c1, c2, cf.ints)
		case DtypeFloat:
			if cf.floats == nil {
				return nil, err
			}
			return compFactoryFloats(c1, c2, cf.floats)
		case DtypeBool:
			if cf.bools == nil {
				return nil, err
			}
			return compFactoryBools(c1, c2, cf.bools)
		case DtypeDate:
			if cf.dates == nil {
				return nil, err
			}
			return compFactoryDates(c1, c2, cf.dates)
		case DtypeDatetime:
			if cf.datetimes == nil {
				return nil, err
			}
			return compFactoryDatetimes(c1, c2, cf.datetimes)
		default:
			return nil, err
		}
	}

	type dtypes struct{ a, b Dtype }
	c1d := c1.dtype
	c2d := c2.dtype
	cs := dtypes{c1d, c2d}
	switch cs {
	case dtypes{DtypeInt, DtypeFloat}:
		if cf.intfloat == nil {
			return nil, err
		}
		return compFactoryIntsFloats(c1, c2, cf.intfloat)
	case dtypes{DtypeFloat, DtypeInt}:
		if cf.floatint == nil {
			return nil, err
		}
		return compFactoryFloatsInts(c1, c2, cf.floatint)
	default:
		return nil, err

	}
}

// EvalAnd produces a bitwise operation on two bool chunks
func EvalAnd(c1 *Chunk, c2 *Chunk) (*Chunk, error) {
	return compEval(c1, c2, compFuncs{
		bools: func(a, b uint64) uint64 { return a & b },
	})
}

// EvalOr produces a bitwise operation on two bool chunks
func EvalOr(c1 *Chunk, c2 *Chunk) (*Chunk, error) {
	return compEval(c1, c2, compFuncs{
		bools: func(a, b uint64) uint64 { return a | b },
	})
}

// EvalEq compares values from two different chunks
func EvalEq(c1 *Chunk, c2 *Chunk) (*Chunk, error) {
	return compEval(c1, c2, compFuncs{
		ints:      func(a, b int64) bool { return a == b },
		floats:    func(a, b float64) bool { return a == b },
		intfloat:  func(a int64, b float64) bool { return float64(a) == b },
		floatint:  func(a float64, b int64) bool { return a == float64(b) },
		strings:   func(a, b string) bool { return a == b },
		bools:     func(a, b uint64) uint64 { return a ^ (^b) },
		dates:     DatesEqual,
		datetimes: DatetimesEqual,
	})
}

// EvalNeq compares values from two different chunks for inequality
func EvalNeq(c1 *Chunk, c2 *Chunk) (*Chunk, error) {
	return compEval(c1, c2, compFuncs{
		ints:      func(a, b int64) bool { return a != b },
		floats:    func(a, b float64) bool { return a != b },
		intfloat:  func(a int64, b float64) bool { return float64(a) != b },
		floatint:  func(a float64, b int64) bool { return a != float64(b) },
		strings:   func(a, b string) bool { return a != b },
		bools:     func(a, b uint64) uint64 { return a ^ b },
		dates:     DatesNotEqual,
		datetimes: DatetimesNotEqual,
	})
}

// EvalGt checks if values in c1 are greater than in c2
func EvalGt(c1 *Chunk, c2 *Chunk) (*Chunk, error) {
	return compEval(c1, c2, compFuncs{
		ints:      func(a, b int64) bool { return a > b },
		floats:    func(a, b float64) bool { return a > b },
		intfloat:  func(a int64, b float64) bool { return float64(a) > b },
		floatint:  func(a float64, b int64) bool { return a > float64(b) },
		strings:   func(a, b string) bool { return a > b },
		bools:     func(a, b uint64) uint64 { return a & (^b) },
		dates:     DatesGreaterThan,
		datetimes: DatetimesGreaterThan,
	})
}

// EvalGte checks if values in c1 are greater than or equal to those in c2
func EvalGte(c1 *Chunk, c2 *Chunk) (*Chunk, error) {
	return compEval(c1, c2, compFuncs{
		ints:      func(a, b int64) bool { return a >= b },
		floats:    func(a, b float64) bool { return a >= b },
		intfloat:  func(a int64, b float64) bool { return float64(a) >= b },
		floatint:  func(a float64, b int64) bool { return a >= float64(b) },
		strings:   func(a, b string) bool { return a >= b },
		bools:     func(a, b uint64) uint64 { return (a & (^b)) | (a ^ (^b)) },
		dates:     DatesGreaterThanEqual,
		datetimes: DatetimesGreaterThanEqual,
	})
}

// EvalLt checks if values in c1 are lower than in c2
func EvalLt(c1 *Chunk, c2 *Chunk) (*Chunk, error) {
	return EvalGt(c2, c1)
}

// EvalLte checks if values in c1 are lower than or equal to those in c2
func EvalLte(c1 *Chunk, c2 *Chunk) (*Chunk, error) {
	return EvalGte(c2, c1)
}

// ARCH: either get rid of all this via generic, or, better yet, rewrite all the algebraics
// using functions. We could then, like in Julia (or lisps), have a function -(a, b)
type algebraFuncs struct {
	ints     func(int64, int64) int64
	floats   func(float64, float64) float64
	intfloat func(int64, float64) float64
	floatint func(float64, int64) float64
}

func algebraFactoryInts(c1 *Chunk, c2 *Chunk, compFn func(int64, int64) int64) (*Chunk, error) {
	nvals := c1.Len()

	if c1.IsLiteral && c2.IsLiteral {
		// OPTIM: this should be a part of constant folding and should never get to this point
		val := compFn(c1.storage.ints[0], c2.storage.ints[0])
		return NewChunkLiteralInts(val, nvals), nil
	}
	var eval func(j int) int64
	eval = func(j int) int64 { return compFn(c1.storage.ints[j], c2.storage.ints[j]) }
	if c1.IsLiteral {
		val := c1.storage.ints[0]
		eval = func(j int) int64 { return compFn(val, c2.storage.ints[j]) }
	}
	if c2.IsLiteral {
		val := c2.storage.ints[0]
		eval = func(j int) int64 { return compFn(c1.storage.ints[j], val) }
	}
	ret := make([]int64, nvals)
	for j := 0; j < nvals; j++ {
		ret[j] = eval(j)
	}
	return intChunkFromParts(ret, c1.Nullability, c2.Nullability), nil
}

func algebraFactoryFloats(c1 *Chunk, c2 *Chunk, compFn func(float64, float64) float64) (*Chunk, error) {
	nvals := c1.Len()

	if c1.IsLiteral && c2.IsLiteral {
		// OPTIM: this should be a part of constant folding and should never get to this point
		val := compFn(c1.storage.floats[0], c2.storage.floats[0])
		return NewChunkLiteralFloats(val, nvals), nil
	}
	var eval func(j int) float64
	eval = func(j int) float64 { return compFn(c1.storage.floats[j], c2.storage.floats[j]) }
	if c1.IsLiteral {
		val := c1.storage.floats[0]
		eval = func(j int) float64 { return compFn(val, c2.storage.floats[j]) }
	}
	if c2.IsLiteral {
		val := c2.storage.floats[0]
		eval = func(j int) float64 { return compFn(c1.storage.floats[j], val) }
	}
	ret := make([]float64, nvals)
	for j := 0; j < nvals; j++ {
		ret[j] = eval(j)
	}
	return floatChunkFromParts(ret, c1.Nullability, c2.Nullability), nil
}

// ARCH: this is identical to `algebraFactoryFloats` apart from the compFn signature in the argument
func algebraFactoryIntFloat(c1 *Chunk, c2 *Chunk, compFn func(int64, float64) float64) (*Chunk, error) {
	nvals := c1.Len()

	if c1.IsLiteral && c2.IsLiteral {
		// OPTIM: this should be a part of constant folding and should never get to this point
		val := compFn(c1.storage.ints[0], c2.storage.floats[0])
		return NewChunkLiteralFloats(val, nvals), nil
	}
	var eval func(j int) float64
	eval = func(j int) float64 { return compFn(c1.storage.ints[j], c2.storage.floats[j]) }
	if c1.IsLiteral {
		val := c1.storage.ints[0]
		eval = func(j int) float64 { return compFn(val, c2.storage.floats[j]) }
	}
	if c2.IsLiteral {
		val := c2.storage.floats[0]
		eval = func(j int) float64 { return compFn(c1.storage.ints[j], val) }
	}
	ret := make([]float64, nvals)
	for j := 0; j < nvals; j++ {
		ret[j] = eval(j)
	}
	return floatChunkFromParts(ret, c1.Nullability, c2.Nullability), nil
}

// ARCH: this is identical to `algebraFactoryFloats` apart from the compFn signature in the argument
func algebraFactoryFloatInt(c1 *Chunk, c2 *Chunk, compFn func(float64, int64) float64) (*Chunk, error) {
	nvals := c1.Len()

	if c1.IsLiteral && c2.IsLiteral {
		// OPTIM: this should be a part of constant folding and should never get to this point
		val := compFn(c1.storage.floats[0], c2.storage.ints[0])
		return NewChunkLiteralFloats(val, nvals), nil
	}
	var eval func(j int) float64
	eval = func(j int) float64 { return compFn(c1.storage.floats[j], c2.storage.ints[j]) }
	if c1.IsLiteral {
		val := c1.storage.floats[0]
		eval = func(j int) float64 { return compFn(val, c2.storage.ints[j]) }
	}
	if c2.IsLiteral {
		val := c2.storage.ints[0]
		eval = func(j int) float64 { return compFn(c1.storage.floats[j], val) }
	}
	ret := make([]float64, nvals)
	for j := 0; j < nvals; j++ {
		ret[j] = eval(j)
	}
	return floatChunkFromParts(ret, c1.Nullability, c2.Nullability), nil
}

func algebraicEval(c1 *Chunk, c2 *Chunk, commutative bool, cf algebraFuncs) (*Chunk, error) {
	errg := func(c1d, c2d Dtype) error {
		return fmt.Errorf("algebraic expression not supported for types %s and %s: %w", c1d, c2d, errProjectionNotSupported)
	}
	c1d := c1.dtype
	c2d := c2.dtype
	if c1d == c2d {
		switch c1d {
		case DtypeInt:
			return algebraFactoryInts(c1, c2, cf.ints)
		case DtypeFloat:
			return algebraFactoryFloats(c1, c2, cf.floats)
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
			return algebraFactoryFloatInt(c2, c1, cf.floatint)
		}
		return algebraFactoryIntFloat(c1, c2, cf.intfloat)
	case dtypes{DtypeFloat, DtypeInt}:
		if commutative && cf.floatint == nil {
			return algebraFactoryIntFloat(c2, c1, cf.intfloat)
		}
		return algebraFactoryFloatInt(c1, c2, cf.floatint)
	default:
		return nil, errg(c1d, c2d)
	}
}

// a solid case for generics?
func EvalAdd(c1 *Chunk, c2 *Chunk) (*Chunk, error) {
	return algebraicEval(c1, c2, true, algebraFuncs{
		ints:     func(a, b int64) int64 { return a + b },
		floats:   func(a, b float64) float64 { return a + b },
		intfloat: func(a int64, b float64) float64 { return float64(a) + b }, // commutative
	})
}

func EvalSubtract(c1 *Chunk, c2 *Chunk) (*Chunk, error) {
	return algebraicEval(c1, c2, false, algebraFuncs{
		ints:     func(a, b int64) int64 { return a - b },
		floats:   func(a, b float64) float64 { return a - b },
		intfloat: func(a int64, b float64) float64 { return float64(a) - b }, // commutative only with a multiplication
		floatint: func(a float64, b int64) float64 { return a - float64(b) },
	})
}

// different return type for ints! should we perhaps cast to make this more systematic?
// check for division by zero (gives +- infty, which will break json?)
func EvalDivide(c1 *Chunk, c2 *Chunk) (*Chunk, error) {
	return algebraicEval(c1, c2, false, algebraFuncs{
		ints:     func(a, b int64) int64 { return a / b },
		floats:   func(a, b float64) float64 { return a / b },
		intfloat: func(a int64, b float64) float64 { return float64(a) / b }, // not commutative
		floatint: func(a float64, b int64) float64 { return a / float64(b) },
	})
}

func EvalMultiply(c1 *Chunk, c2 *Chunk) (*Chunk, error) {
	return algebraicEval(c1, c2, true, algebraFuncs{
		ints:     func(a, b int64) int64 { return a * b },
		floats:   func(a, b float64) float64 { return a * b },
		intfloat: func(a int64, b float64) float64 { return float64(a) * b }, // commutative
	})
}
