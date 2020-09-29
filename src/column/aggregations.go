package column

import (
	"fmt"

	"github.com/kokes/smda/src/bitmap"
)

type AggState struct {
	dtype   Dtype
	ints    []int64
	floats  []float64
	strings []string
	// TODO: ... strings? bools?
	counts   []int64
	AddChunk func(buckets []uint64, ndistinct int, data Chunk)
	Resolve  func() (Chunk, error)
}

// how will we update the state given a value
type updateFuncs struct {
	ints    func(state *AggState, value int64, position uint64)
	floats  func(state *AggState, value float64, position uint64)
	strings func(state *AggState, value string, position uint64)
}

// given our state, how do we generate chunks?
type resolveFuncs struct {
	any     func(state *AggState) func() (Chunk, error)
	ints    func(state *AggState) func() (Chunk, error)
	floats  func(state *AggState) func() (Chunk, error)
	strings func(state *AggState) func() (Chunk, error)
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
	strings: func(agg *AggState) func() (Chunk, error) {
		return func() (Chunk, error) {
			bm := bitmapFromCounts(agg.counts)
			return newChunkStringsFromSlice(agg.strings, bm), nil
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
		resolvers := resolveFuncs{}
		switch function {
		case "count":
			if len(dtypes) == 0 {
				state.dtype = DtypeInt // count() will count integers
			} else {
				state.dtype = dtypes[0] // count(expr) will accept type(expr)
			}
			resolvers = resolveFuncs{
				any: func(agg *AggState) func() (Chunk, error) {
					return func() (Chunk, error) {
						return newChunkIntsFromSlice(agg.counts, nil), nil
					}
				},
			}
		case "min":
			state.dtype = dtypes[0]
			updaters.ints = func(agg *AggState, val int64, pos uint64) {
				if agg.counts[pos] == 0 || val < agg.ints[pos] {
					agg.ints[pos] = val
				}
			}
			updaters.floats = func(agg *AggState, val float64, pos uint64) {
				if agg.counts[pos] == 0 || val < agg.floats[pos] {
					agg.floats[pos] = val
				}
			}
			updaters.strings = func(agg *AggState, val string, pos uint64) {
				if agg.counts[pos] == 0 || val < agg.strings[pos] {
					agg.strings[pos] = val
				}
			}
			resolvers = genericResolvers
		case "max":
			state.dtype = dtypes[0]
			updaters.ints = func(agg *AggState, val int64, pos uint64) {
				if agg.counts[pos] == 0 || val > agg.ints[pos] {
					agg.ints[pos] = val
				}
			}
			updaters.floats = func(agg *AggState, val float64, pos uint64) {
				if agg.counts[pos] == 0 || val > agg.floats[pos] {
					agg.floats[pos] = val
				}
			}
			resolvers = genericResolvers
		case "sum":
			state.dtype = dtypes[0]
			updaters.ints = func(agg *AggState, val int64, pos uint64) {
				agg.ints[pos] += val
			}
			updaters.floats = func(agg *AggState, val float64, pos uint64) {
				agg.floats[pos] += val
			}
			resolvers = genericResolvers
		case "avg":
			state.dtype = dtypes[0]
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
		adder, err := adderFactory(state, updaters)
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
func ensureLengthInts(data []int64, length int) []int64 {
	currentLength := len(data)
	if currentLength >= length {
		return data
	}
	data = append(data, make([]int64, length-currentLength)...)
	return data
}
func ensureLengthFloats(data []float64, length int) []float64 {
	currentLength := len(data)
	if currentLength >= length {
		return data
	}
	data = append(data, make([]float64, length-currentLength)...)
	return data
}

func ensurelengthStrings(data []string, length int) []string {
	currentLength := len(data)
	if currentLength >= length {
		return data
	}
	data = append(data, make([]string, length-currentLength)...)
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

// OPTIM/ARCH: this might be abstracted away thanks to generics (though... we don't have nthvalue for all chunk types)
func adderFactory(agg *AggState, upd updateFuncs) (func([]uint64, int, Chunk), error) {
	switch agg.dtype {
	case DtypeInt:
		return func(buckets []uint64, ndistinct int, data Chunk) {
			agg.counts = ensureLengthInts(agg.counts, ndistinct)
			agg.ints = ensureLengthInts(agg.ints, ndistinct)

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
				// we don't always have updaters (e.g. for counters)
				// OPTIM: can we hoist this outside the loop?
				if upd.ints != nil {
					upd.ints(agg, val, pos)
				}
				agg.counts[pos]++
			}
		}, nil
	case DtypeFloat:
		return func(buckets []uint64, ndistinct int, data Chunk) {
			agg.counts = ensureLengthInts(agg.counts, ndistinct)
			agg.floats = ensureLengthFloats(agg.floats, ndistinct)

			rc := data.(*ChunkFloats)
			for j, val := range rc.data {
				if rc.nullability != nil && rc.nullability.Get(j) {
					continue
				}
				pos := buckets[j]
				if upd.floats != nil {
					upd.floats(agg, val, pos)
				}
				agg.counts[pos]++
			}
		}, nil
	case DtypeString:
		return func(buckets []uint64, ndistinct int, data Chunk) {
			agg.counts = ensureLengthInts(agg.counts, ndistinct)
			agg.strings = ensurelengthStrings(agg.strings, ndistinct)

			rc := data.(*ChunkStrings)
			for j := 0; j < rc.Len(); j++ {
				if rc.nullability != nil && rc.nullability.Get(j) {
					continue
				}
				val := rc.nthValue(j)
				pos := buckets[j]
				if upd.strings != nil {
					upd.strings(agg, val, pos)
				}
				agg.counts[pos]++
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
	case DtypeString:
		return resfuncs.strings(agg), nil
	default:
		return nil, fmt.Errorf("resolver for type %v not supported", agg.dtype)
	}
}
