package column

import (
	"fmt"
	"math"

	"github.com/kokes/smda/src/bitmap"
)

// FuncAgg maps aggregating function names to their implementations
// inspiration: pg docs (included all but xml and json)
// https://www.postgresql.org/docs/12/functions-aggregate.html
var blankConstructor = func() Aggregator { return nil }
var FuncAgg = map[string]func() Aggregator{
	// won't support:
	// "array_agg": blankConstructor, // don't have an array type
	// "every":      blankConstructor, // alias for bit_and
	// "bit_and": blankConstructor, "bit_or": blankConstructor, // probably not terribly useful
	"avg":        blankConstructor,
	"bool_and":   blankConstructor,
	"bool_or":    blankConstructor,
	"count":      blankConstructor, // * or Expression
	"min":        func() Aggregator { return &AggMin{} },
	"max":        blankConstructor,
	"sum":        blankConstructor,
	"string_agg": blankConstructor, // the only aggregator with a parameter
}

type Aggregator interface {
	AddChunk(buckets []uint64, ndistinct int, data Chunk) error
	Resolve() (Chunk, error)
}

// ARCH/TODO: abstract this out using generics
func ensureLengthInts(data []int64, length int, sentinel int64) []int64 {
	currentLength := len(data)
	if currentLength >= length {
		return data
	}
	data = append(data, make([]int64, length-currentLength)...)
	if sentinel == 0 {
		return data
	}
	for j := currentLength; j < length; j++ {
		data[j] = sentinel
	}
	return data
}
func ensureLengthFloats(data []float64, length int, sentinel float64) []float64 {
	currentLength := len(data)
	if currentLength >= length {
		return data
	}
	data = append(data, make([]float64, length-currentLength)...)
	if sentinel == 0 {
		return data
	}
	for j := currentLength; j < length; j++ {
		data[j] = sentinel
	}
	return data
}

// used to convert a counts slice (how many rows are there for a given bucket) to a nullability
// bitmap - so a NULL (1) for each zero value
func bitmapFromCounts(counts []int64) *bitmap.Bitmap {
	var bm *bitmap.Bitmap
	for j, el := range counts {
		if el == 0 {
			if bm == nil {
				bm = bitmap.NewBitmap(len(counts))
			}
			bm.Set(j, true)
		}
	}
	return bm
}

// ARCH: this will work nicely when we get generics - one for ints,
// one for floats, though we'll need to be careful about INT_MAX and FLOAT_MAX
type AggMin struct {
	dtype    Dtype
	minInt   []int64
	minFloat []float64
	// TODO: ... strings? bools?
	// ARCH: could have used a bitmap instead, but this helps us in other ways (debugging, mostly)
	counts []int64
}

func (agg *AggMin) AddChunk(buckets []uint64, ndistinct int, data Chunk) error {
	// TODO: since we don't have a constructor any more, we need to initialise agg.dtype somehow
	if agg.dtype == DtypeInvalid {
		agg.dtype = data.Dtype()
	}

	// if agg.dtype != data.Dtype() {
	// 	err...
	// }

	// TODO: we need to test this on multiple stripes (and one that introduces
	// many new distinct values in new stripes)
	agg.counts = ensureLengthInts(agg.counts, ndistinct, 0)
	switch agg.dtype {
	case DtypeInt:
		agg.minInt = ensureLengthInts(agg.minInt, ndistinct, math.MaxInt64)
	case DtypeFloat:
		agg.minFloat = ensureLengthFloats(agg.minFloat, ndistinct, math.MaxFloat64)
	default:
		return errTypeNotSupported
	}

	switch data.Dtype() {
	case DtypeInt:
		rc := data.(*ChunkInts)
		for j, val := range rc.data {
			// OPTIM: we could construct a new loop without this check for rc.nullability == nil,
			// so we'd save a lot of ifs at the cost of a lot of code duplication (also, branch prediction
			// is pretty good these days)
			if rc.nullability != nil && rc.nullability.Get(j) {
				continue
			}
			pos := buckets[j]
			agg.counts[pos]++
			minval := agg.minInt[pos]
			if val < minval {
				agg.minInt[pos] = val
			}
		}
	case DtypeFloat:
		rc := data.(*ChunkFloats)
		for j, val := range rc.data {
			// OPTIM: we could construct a new loop without this check for rc.nullability == nil,
			// so we'd save a lot of ifs at the cost of a lot of code duplication (also, branch prediction
			// is pretty good these days)
			if rc.nullability != nil && rc.nullability.Get(j) {
				continue
			}
			pos := buckets[j]
			agg.counts[pos]++
			minval := agg.minFloat[pos]
			if val < minval {
				agg.minFloat[pos] = val
			}
		}
	default:
		return fmt.Errorf("%w: cannot run AddChunk on %v", errTypeNotSupported, data.Dtype())
	}
	return nil
}

func (agg *AggMin) Resolve() (Chunk, error) {
	bm := bitmapFromCounts(agg.counts)
	switch agg.dtype {
	case DtypeInt:
		return newChunkIntsFromSlice(agg.minInt, bm), nil
	case DtypeFloat:
		return newChunkFloatsFromSlice(agg.minFloat, bm), nil
	default:
		return nil, fmt.Errorf("%w: cannot run Resolve on %v", errTypeNotSupported, agg.dtype)
	}
}
