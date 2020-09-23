package column

import (
	"fmt"
	"math"

	"github.com/kokes/smda/src/bitmap"
)

type AggState struct {
	dtype  Dtype
	ints   []int64
	floats []float64
	// TODO: ... strings? bools?
	counts   []int64
	AddChunk func(buckets []uint64, ndistinct int, data Chunk)
	Resolve  func() (Chunk, error)
}

// how will we update the state given a value
type updateFuncs struct {
	ints   func(state *AggState, value int64, position uint64)
	floats func(state *AggState, value float64, position uint64)
}

// what state defaults should be filled in (e.g. for min() it's math.MAX)
// ARCH: should perhaps be called `defaults` or something, because these
// are not really sentinels
type sentinels struct {
	ints   int64
	floats float64
}

// given our state, how do we generate chunks?
type resolveFuncs struct {
	any    func(state *AggState) func() (Chunk, error)
	ints   func(state *AggState) func() (Chunk, error)
	floats func(state *AggState) func() (Chunk, error)
}

// these resolvers don't do much, they just take our state and make it into Chunks
// and so are not suitable for e.g. avg(), where some finaliser work needs to be done
var genericResolvers = resolveFuncs{
	ints: func(agg *AggState) func() (Chunk, error) {
		return func() (Chunk, error) {
			bm := bitmapFromCounts(agg.counts)
			return newChunkIntsFromSlice(agg.ints, bm), nil
		}
	},
	floats: func(agg *AggState) func() (Chunk, error) {
		return func() (Chunk, error) {
			bm := bitmapFromCounts(agg.counts)
			return newChunkFloatsFromSlice(agg.floats, bm), nil
		}
	},
}

// NewAggregator implements a constructor for various aggregating functions.
// We got inspired by Postgres' functions https://www.postgresql.org/docs/12/functions-aggregate.html
//   - not implemented: xml/json functions (don't have the data types), array_agg (no arrays),
//					    every (just an alias), bit_and/bit_or (doesn't seem useful for us)
//   - implemented: min, max, sum, avg, count
//   - planned: bool_and, bool_or, string_agg
//   - thinking: countDistinct, sketch-based approxCountDistinct
// ARCH: function string -> uint8 const?
// dtypes are types of inputs - rename?
// TODO: check for function existence
// OPTIM: the switch(function) could be hoisted outside the closure (would work as a function existence validator)
func NewAggregator(function string) (func(...Dtype) (*AggState, error), error) {
	return func(dtypes ...Dtype) (*AggState, error) {
		// TODO: check dtypes length? (though that should have been done in return_types already)
		state := &AggState{}
		updaters := updateFuncs{}
		sents := sentinels{}
		resolvers := resolveFuncs{}
		switch function {
		case "count":
			if len(dtypes) == 0 {
				state.dtype = DtypeInt // count() will count integers
			} else {
				state.dtype = dtypes[0] // count(expr) will accept type(expr)
			}
			sents = sentinels{} // zeroes are fine
			resolvers = resolveFuncs{
				any: func(agg *AggState) func() (Chunk, error) {
					return func() (Chunk, error) {
						return newChunkIntsFromSlice(agg.counts, nil), nil
					}
				},
			}
		case "min":
			state.dtype = dtypes[0]
			sents = sentinels{ints: math.MaxInt64, floats: math.Inf(1)}
			updaters.ints = func(agg *AggState, val int64, pos uint64) {
				if val < agg.ints[pos] {
					agg.ints[pos] = val
				}
			}
			updaters.floats = func(agg *AggState, val float64, pos uint64) {
				if val < agg.floats[pos] {
					agg.floats[pos] = val
				}
			}
			resolvers = genericResolvers
		case "max":
			state.dtype = dtypes[0]
			sents = sentinels{ints: math.MinInt64, floats: math.Inf(-1)}
			updaters.ints = func(agg *AggState, val int64, pos uint64) {
				if val > agg.ints[pos] {
					agg.ints[pos] = val
				}
			}
			updaters.floats = func(agg *AggState, val float64, pos uint64) {
				if val > agg.floats[pos] {
					agg.floats[pos] = val
				}
			}
			resolvers = genericResolvers
		case "sum":
			state.dtype = dtypes[0]
			sents = sentinels{} // zeroes are fine
			updaters.ints = func(agg *AggState, val int64, pos uint64) {
				agg.ints[pos] += val
			}
			updaters.floats = func(agg *AggState, val float64, pos uint64) {
				agg.floats[pos] += val
			}
			resolvers = genericResolvers
		case "avg":
			state.dtype = dtypes[0]
			sents = sentinels{} // zeroes are fine
			// OPTIM/ARCH: this is not the best way to average out, see specialised algorithms
			updaters.ints = func(agg *AggState, val int64, pos uint64) {
				agg.ints[pos] += val
			}
			updaters.floats = func(agg *AggState, val float64, pos uint64) {
				agg.floats[pos] += val
			}
			// so far it's the same as sums, so we might share the codebase somehow (fallthrough and overwrite resolvers?)
			resolvers = resolveFuncs{
				ints: func(agg *AggState) func() (Chunk, error) {
					return func() (Chunk, error) {
						bm := bitmapFromCounts(agg.counts)
						avgs := make([]float64, len(agg.ints))
						for j, el := range agg.ints {
							avgs[j] = float64(el) / float64(agg.counts[j]) // el/0 will yield a +-inf, but that's fine
						}
						return newChunkFloatsFromSlice(avgs, bm), nil
					}
				},
				floats: func(agg *AggState) func() (Chunk, error) {
					return func() (Chunk, error) {
						bm := bitmapFromCounts(agg.counts)
						// we can overwrite our float sums in place, we no longer need them
						for j, el := range agg.floats {
							agg.floats[j] = el / float64(agg.counts[j])
						}
						return newChunkFloatsFromSlice(agg.floats, bm), nil
					}
				},
			}
		default:
			// TODO: custom error?
			return nil, fmt.Errorf("aggregation not supported: %v", function)
		}
		adder, err := adderFactory(state, updaters, sents)
		if err != nil {
			return nil, err
		}
		state.AddChunk = adder
		resolver, err := resolverFactory(state, resolvers)
		if err != nil {
			return nil, err
		}
		state.Resolve = resolver
		return state, nil
	}, nil
}

// ARCH/TODO: abstract this out using generics
// will probably look something like this:
// type extendable interface {
// 	type int, int64, float64, bool, string
// }
// func ensureLength[T extendable](data []T, length int, sentinel T) []T {
// 	currentLength := len(data)
// 	if currentLength >= length {
// 		return data
// 	}
// 	data = append(data, make([]T, length-currentLength)...)
// 	// probably cannot express default value checks in generics?
// 	//if sentinel == 0 {
// 	//	return data
// 	//}
// 	for j := currentLength; j < length; j++ {
// 		data[j] = sentinel
// 	}
// 	return data
// }
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

func adderFactory(agg *AggState, upd updateFuncs, sents sentinels) (func([]uint64, int, Chunk), error) {
	switch agg.dtype {
	case DtypeInt:
		return func(buckets []uint64, ndistinct int, data Chunk) {
			agg.counts = ensureLengthInts(agg.counts, ndistinct, 0)
			agg.ints = ensureLengthInts(agg.ints, ndistinct, sents.ints)

			// this can happen if there are no children - so just update the counters
			// this is here specifically for `count()`
			if data == nil {
				for _, pos := range buckets {
					agg.counts[pos]++
				}
				return
			}
			rc := data.(*ChunkInts)
			for j, val := range rc.data {
				if rc.nullability != nil && rc.nullability.Get(j) {
					continue
				}
				pos := buckets[j]
				agg.counts[pos]++
				// we don't always have updaters (e.g. for counters)
				// OPTIM: can we hoist this outside the loop?
				if upd.ints != nil {
					upd.ints(agg, val, pos)
				}
			}
		}, nil
	case DtypeFloat:
		return func(buckets []uint64, ndistinct int, data Chunk) {
			agg.counts = ensureLengthInts(agg.counts, ndistinct, 0)
			agg.floats = ensureLengthFloats(agg.floats, ndistinct, sents.floats)

			rc := data.(*ChunkFloats)
			for j, val := range rc.data {
				if rc.nullability != nil && rc.nullability.Get(j) {
					continue
				}
				pos := buckets[j]
				agg.counts[pos]++
				if upd.floats != nil {
					upd.floats(agg, val, pos)
				}
			}
		}, nil
	default:
		return nil, fmt.Errorf("adder factory not supported for %v", agg.dtype)
	}
}

// TODO/ARCH: we don't test that the individual resolvers exist - may panic at runtime
func resolverFactory(agg *AggState, resfuncs resolveFuncs) (func() (Chunk, error), error) {
	if resfuncs.any != nil {
		return resfuncs.any(agg), nil
	}
	switch agg.dtype {
	case DtypeInt:
		return resfuncs.ints(agg), nil
	case DtypeFloat:
		return resfuncs.floats(agg), nil
	default:
		return nil, fmt.Errorf("resolver for type %v not supported", agg.dtype)
	}
}
