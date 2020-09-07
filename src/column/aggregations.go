package column

import (
	"fmt"
)

// FuncAgg maps aggregating function names to their implementations
// inspiration: pg docs (included all but xml and json)
// https://www.postgresql.org/docs/12/functions-aggregate.html
var FuncAgg = map[string]func(int, Dtype) Aggregator{
	"array_agg": nil,
	"avg":       nil,
	"bit_and":   nil, "bit_or": nil, // useful?
	"bool_and":   nil,
	"bool_or":    nil,
	"count":      nil, // * or Expression
	"every":      nil,
	"min":        NewAggMin,
	"max":        nil,
	"sum":        nil,
	"string_agg": nil, // the only aggregator with a parameter
}

type Aggregator interface {
	AddChunk(buckets []uint64, data Chunk) error
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

func NewAggMin(n int, dtype Dtype) Aggregator {
	counts := make([]int, n)
	agg := &AggMin{dtype: dtype, counts: counts}
	switch dtype {
	case DtypeInt:
		agg.minInt = make([]int64, n)
	case DtypeFloat:
		agg.minFloat = make([]float64, n)
	default:
		panic(fmt.Sprintf("type %v not supported", dtype))
	}
	return agg
}

func (agg *AggMin) AddChunk(buckets []uint64, data Chunk) error {
	switch agg.dtype {
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
		return fmt.Errorf("%w: cannot run AddChunk on %v", errTypeNotSupported, agg.dtype)
	}
	return nil
}
func (agg *AggMin) Resolve() (Chunk, error) {

	switch agg.dtype {
	case DtypeInt:
		// newcolumnints(agg.minInt, bm)
	}
	panic("resolve not finished")
	return nil, nil
}
