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
	"array_agg": blankConstructor,
	"avg":       blankConstructor,
	"bit_and":   blankConstructor, "bit_or": blankConstructor, // useful?
	"bool_and":   blankConstructor,
	"bool_or":    blankConstructor,
	"count":      blankConstructor, // * or Expression
	"every":      blankConstructor,
	"min":        func() Aggregator { return &AggMin{} },
	"max":        blankConstructor,
	"sum":        blankConstructor,
	"string_agg": blankConstructor, // the only aggregator with a parameter
}

type Aggregator interface {
	AddChunk(buckets []uint64, ndistinct int, data Chunk) error
	Resolve() (Chunk, error)
}

// Let's have one constructor that we'll call at parse time?
// NewAggregator(funcname, size) -> Aggregator, save it in Expression.Aggfunc

// Where to resolve types? In return_types.go, or shall we have one more
// method that will accept input dtypes and report the return type? (we could
// use that for normal functions as well, but we don't have individual go types there)

// It's important to return NULL for empty buckets (apart from COUNT, that returns zero)

// ARCH: this will work nicely when we get generics - one for ints,
// one for floats, though we'll need to be careful about INT_MAX and FLOAT_MAX
type AggMin struct {
	dtype    Dtype
	minInt   []int64
	minFloat []float64
	// TODO: ... strings? bools?
	// ARCH: would a bitmap be sufficient?
	counts []int
}

func (agg *AggMin) AddChunk(buckets []uint64, ndistinct int, data Chunk) error {
	// TODO: since we don't have a constructor any more, we need to make sure min{Int,Float} and counts are
	// both big enough for our number of buckets
	if agg.dtype == DtypeInvalid {
		agg.dtype = data.Dtype()
	}

	// if agg.dtype != data.Dtype() {
	// 	err...
	// }

	// TODO: we need to test this on multiple stripes (and one that introduces
	// many new distinct values in new stripes)
	if len(agg.counts) < ndistinct {
		agg.counts = append(agg.counts, make([]int, ndistinct-len(agg.counts))...)
		switch agg.dtype {
		case DtypeInt:
			// ARCH: these snippets can be abstracted away as `ensure(slice, length, sentinel)`
			//       as they'll be helpful in `max` and other agg functions
			currentLength := len(agg.minInt)
			agg.minInt = append(agg.minInt, make([]int64, ndistinct-currentLength)...)
			for j := currentLength; j < ndistinct; j++ {
				agg.minInt[j] = math.MaxInt64
			}
		case DtypeFloat:
			currentLength := len(agg.minFloat)
			agg.minFloat = append(agg.minFloat, make([]float64, ndistinct-currentLength)...)
			for j := currentLength; j < ndistinct; j++ {
				agg.minFloat[j] = math.MaxFloat64
			}
		default:
			return errTypeNotSupported
		}
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
	var bm *bitmap.Bitmap
	for j, el := range agg.counts {
		if el == 0 {
			if bm == nil {
				bm = bitmap.NewBitmap(len(agg.counts))
			}
			bm.Set(j, true)
		}
	}
	switch agg.dtype {
	case DtypeInt:
		return newChunkIntsFromSlice(agg.minInt, bm), nil
	case DtypeFloat:
		return newChunkFloatsFromSlice(agg.minFloat, bm), nil
	default:
		return nil, fmt.Errorf("%w: cannot run Resolve on %v", errTypeNotSupported, agg.dtype)
	}
}