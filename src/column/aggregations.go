package column

import (
	"errors"
	"fmt"

	"github.com/kokes/smda/src/bitmap"
)

var errInvalidAggregation = errors.New("aggregation does not exist")

type AggState struct {
	inputType Dtype
	ints      []int64
	floats    []float64
	strings   []string
	dates     []date
	datetimes []datetime
	counts    []int64
	AddChunk  func(buckets []uint64, ndistinct int, data Chunk)
	Resolve   func() (Chunk, error)
}

// how will we update the state given a value
type updateFuncs struct {
	ints      func(state *AggState, value int64, position uint64)
	floats    func(state *AggState, value float64, position uint64)
	dates     func(state *AggState, value date, position uint64)
	datetimes func(state *AggState, value datetime, position uint64)
	strings   func(state *AggState, value string, position uint64)
}

type resolveFunc func(state *AggState) func() (Chunk, error)

// given our state, how do we generate chunks?
type resolveFuncs struct {
	any       resolveFunc
	ints      resolveFunc
	floats    resolveFunc
	dates     resolveFunc
	datetimes resolveFunc
	strings   resolveFunc
}

// these resolvers don't do much, they just take our state and make it into Chunks
// and so are not suitable for e.g. avg(), where some finaliser work needs to be done
var genericResolvers = resolveFuncs{
	ints: func(agg *AggState) func() (Chunk, error) {
		return func() (Chunk, error) {
			bm := bitmapFromCounts(agg.counts)
			return NewChunkIntsFromSlice(agg.ints, bm), nil
		}
	},
	floats: func(agg *AggState) func() (Chunk, error) {
		return func() (Chunk, error) {
			bm := bitmapFromCounts(agg.counts)
			return NewChunkFloatsFromSlice(agg.floats, bm), nil
		}
	},
	dates: func(agg *AggState) func() (Chunk, error) {
		return func() (Chunk, error) {
			bm := bitmapFromCounts(agg.counts)
			return newChunkDatesFromSlice(agg.dates, bm), nil
		}
	},
	datetimes: func(agg *AggState) func() (Chunk, error) {
		return func() (Chunk, error) {
			bm := bitmapFromCounts(agg.counts)
			return newChunkDatetimesFromSlice(agg.datetimes, bm), nil
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
		state := &AggState{}
		updaters := updateFuncs{}
		resolvers := resolveFuncs{}
		switch function {
		case "count":
			if len(dtypes) == 0 {
				state.inputType = DtypeInt // count() will count integers
			} else {
				state.inputType = dtypes[0] // count(expr) will accept type(expr)
			}
			resolvers = resolveFuncs{
				any: func(agg *AggState) func() (Chunk, error) {
					return func() (Chunk, error) {
						return NewChunkIntsFromSlice(agg.counts, nil), nil
					}
				},
			}
		case "min":
			state.inputType = dtypes[0]
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
			updaters.dates = func(agg *AggState, val date, pos uint64) {
				if agg.counts[pos] == 0 || DatesLessThan(val, agg.dates[pos]) {
					agg.dates[pos] = val
				}
			}
			updaters.datetimes = func(agg *AggState, val datetime, pos uint64) {
				if agg.counts[pos] == 0 || DatetimesLessThan(val, agg.datetimes[pos]) {
					agg.datetimes[pos] = val
				}
			}
			updaters.strings = func(agg *AggState, val string, pos uint64) {
				if agg.counts[pos] == 0 || val < agg.strings[pos] {
					agg.strings[pos] = val
				}
			}
			resolvers = genericResolvers
		case "max":
			state.inputType = dtypes[0]
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
			updaters.dates = func(agg *AggState, val date, pos uint64) {
				if agg.counts[pos] == 0 || DatesGreaterThan(val, agg.dates[pos]) {
					agg.dates[pos] = val
				}
			}
			updaters.datetimes = func(agg *AggState, val datetime, pos uint64) {
				if agg.counts[pos] == 0 || DatetimesGreaterThan(val, agg.datetimes[pos]) {
					agg.datetimes[pos] = val
				}
			}
			updaters.strings = func(agg *AggState, val string, pos uint64) {
				if agg.counts[pos] == 0 || val > agg.strings[pos] {
					agg.strings[pos] = val
				}
			}
			resolvers = genericResolvers
		case "sum":
			state.inputType = dtypes[0]
			updaters.ints = func(agg *AggState, val int64, pos uint64) {
				agg.ints[pos] += val
			}
			updaters.floats = func(agg *AggState, val float64, pos uint64) {
				agg.floats[pos] += val
			}
			resolvers = genericResolvers
		case "avg":
			state.inputType = dtypes[0]
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
						// we can't use agg.ints as we'll return floats
						// if we reuse agg.floats, we can then use a generic resolver
						agg.floats = ensureLengthFloats(agg.floats, len(agg.ints))
						for j, el := range agg.ints {
							agg.floats[j] = float64(el) / float64(agg.counts[j]) // el/0 will yield a +-inf, but that's fine
						}
						return genericResolvers.floats(agg)()
					}
				},
				floats: func(agg *AggState) func() (Chunk, error) {
					return func() (Chunk, error) {
						// we can overwrite our float sums in place, we no longer need them
						for j, el := range agg.floats {
							agg.floats[j] = el / float64(agg.counts[j])
						}
						return genericResolvers.floats(agg)()
					}
				},
			}
		default:
			return nil, fmt.Errorf("%w: %v", errInvalidAggregation, function)
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
func ensureLengthDates(data []date, length int) []date {
	currentLength := len(data)
	if currentLength >= length {
		return data
	}
	data = append(data, make([]date, length-currentLength)...)
	return data
}
func ensureLengthDatetimes(data []datetime, length int) []datetime {
	currentLength := len(data)
	if currentLength >= length {
		return data
	}
	data = append(data, make([]datetime, length-currentLength)...)
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
	switch agg.inputType {
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
				if rc.Nullability != nil && rc.Nullability.Get(j) {
					continue
				}
				pos := buckets[j]
				// TODO(PR): place DISTINCT logic here?
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
				if rc.Nullability != nil && rc.Nullability.Get(j) {
					continue
				}
				pos := buckets[j]
				if upd.floats != nil {
					upd.floats(agg, val, pos)
				}
				agg.counts[pos]++
			}
		}, nil
	case DtypeDate:
		return func(buckets []uint64, ndistinct int, data Chunk) {
			agg.counts = ensureLengthInts(agg.counts, ndistinct)
			agg.dates = ensureLengthDates(agg.dates, ndistinct)

			rc := data.(*ChunkDates)
			for j, val := range rc.data {
				if rc.Nullability != nil && rc.Nullability.Get(j) {
					continue
				}
				pos := buckets[j]
				if upd.floats != nil {
					upd.dates(agg, val, pos)
				}
				agg.counts[pos]++
			}
		}, nil
	case DtypeDatetime:
		return func(buckets []uint64, ndistinct int, data Chunk) {
			agg.counts = ensureLengthInts(agg.counts, ndistinct)
			agg.datetimes = ensureLengthDatetimes(agg.datetimes, ndistinct)

			rc := data.(*ChunkDatetimes)
			for j, val := range rc.data {
				if rc.Nullability != nil && rc.Nullability.Get(j) {
					continue
				}
				pos := buckets[j]
				if upd.floats != nil {
					upd.datetimes(agg, val, pos)
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
				if rc.Nullability != nil && rc.Nullability.Get(j) {
					continue
				}
				val := rc.nthValue(j)
				pos := buckets[j]
				// TODO: if we have a function that "accepts" strings (or other types) but doesn't have an updater for them...
				// this will silently ignore the mismatch (e.g. we didn't have type restrictions on SUM in return_types and we
				// then did a SUM(string) and it silently did nothing)
				if upd.strings != nil {
					upd.strings(agg, val, pos)
				}
				agg.counts[pos]++
			}
		}, nil
	default:
		return nil, fmt.Errorf("adder factory not supported for %v", agg.inputType)
	}
}

func resolverFactory(agg *AggState, resfuncs resolveFuncs) (func() (Chunk, error), error) {
	// the `any` func has precedence over any concrete resolvers
	if resfuncs.any != nil {
		return resfuncs.any(agg), nil
	}
	var rfunc resolveFunc
	switch agg.inputType {
	case DtypeInt:
		rfunc = resfuncs.ints
	case DtypeFloat:
		rfunc = resfuncs.floats
	case DtypeDate:
		rfunc = resfuncs.dates
	case DtypeDatetime:
		rfunc = resfuncs.datetimes
	case DtypeString:
		rfunc = resfuncs.strings
	}
	// we hit this branch if either the type is not in the switch (there's no way we can
	// resolve this type), OR if the function in the struct is nil (undefined)
	if rfunc == nil {
		return nil, fmt.Errorf("resolver for type %v not supported", agg.inputType)
	}
	return rfunc(agg), nil
}
