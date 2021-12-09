// steps to add a new function:
// 1. add an implementation here and add it to FuncProj
// 2. specify its return types (return_types.go)
// 3. test both implementations above
package column

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/kokes/smda/src/bitmap"
)

var errTypeNotSupported = errors.New("type not supported in this function")

// TODO: this will be hard to cover properly, so let's make sure we test everything explicitly
// ARCH: we're not treating literals any differently, but since they share the same backing store
//       as non-literals, we're okay... is that okay?
var FuncProj = map[string]func(...*Chunk) (*Chunk, error){
	"now":      evalNow,
	"version":  evalVersion,
	"nullif":   evalNullIf,
	"coalesce": evalCoalesce,
	"round":    evalRound, // TODO: ceil, floor
	"sin":      numFunc(math.Sin),
	"cos":      numFunc(math.Cos),
	"tan":      numFunc(math.Tan),
	"asin":     numFunc(math.Asin),
	"acos":     numFunc(math.Acos),
	"atan":     numFunc(math.Atan),
	"sinh":     numFunc(math.Sinh),
	"cosh":     numFunc(math.Cosh),
	"tanh":     numFunc(math.Tanh),
	"sqrt":     numFunc(math.Sqrt),
	"exp":      numFunc(math.Exp),
	"exp2":     numFunc(math.Exp2),
	"log":      numFunc(math.Log),
	"log2":     numFunc(math.Log2),
	"log10":    numFunc(math.Log10),
	// TODO: log with arbitrary base
	// ARCH: these string functions are unicode aware and thus e.g. TRIM removes more than just spaces
	// OPTIM: consider avoiding some of the UTF penalty (e.g. strings.TrimSpace has optimisations for this)
	"trim":       stringFunc(strings.TrimSpace),
	"lower":      stringFunc(strings.ToLower),
	"upper":      stringFunc(strings.ToUpper),
	"left":       evalLeft,
	"split_part": evalSplitPart,
	// TODO(next): all those useful string functions - hashing, mid, right, position, ...
}

func evalNow(cs ...*Chunk) (*Chunk, error) {
	dt, err := newDatetimeFromNative(time.Now())
	if err != nil {
		return nil, err
	}
	return NewChunkLiteralDatetimes(dt, 1), nil
}

// TODO: Inject this from a global const or something? Feed that at build time?
func evalVersion(cs ...*Chunk) (*Chunk, error) {
	// ARCH/OPTIM: it should be done this way:
	// return NewChunkLiteralStrings("version_undefined", 1), nil
	// ... but we don't have a good way of testing equivalence of literal columns in query_test.go
	// maybe once we start comparing serialised versions of both, then we'll be able to revert to
	// the implementation above
	return newChunkStringsFromSlice([]string{"version_undefined"}, nil), nil
}

func evalCoalesce(cs ...*Chunk) (*Chunk, error) {
	if len(cs) == 0 {
		// ARCH: this is taken care of in return_types, delete? panic?
		return nil, errors.New("coalesce needs at least one argument")
	}
	if len(cs) == 1 {
		return cs[0], nil
	}
	// OPTIM: if cs[0].IsNullable == false, exit with it (we don't have that method though)
	panic("TODO: not implemented yet")
	// how will we know the schema of this result? should we incorporate the return_type flow here?
	// I guess we can't do that since that would introduce a circular dependency - but we could move
	// coalesceType from `return_types` to `column`, so that we'd just use it here and import it in
	// `return_types`
	// we could, however, make this into a closure, because we don't want to keep determining the
	// output type upon each call... though it will be expensive anyway...?
}

// var EvalAbs = numFunc(math.Abs) // this should probably behave differently for ints

// at some point test sum(nullif([1,2,3], 2)) to check we're not interpreting
// "dead" values
// treat this differently, if cs[0] is a literal column
func evalNullIf(cs ...*Chunk) (*Chunk, error) {
	eq, err := EvalEq(cs[0], cs[1])
	if err != nil {
		return nil, err
	}
	truths := eq.Truths()
	if truths.Count() == 0 {
		return cs[0], nil
	}
	cb := cs[0].Clone()
	cb.Nullify(truths)
	return cb, nil
}

// ARCH: this could be generalised using numFunc, we just have to pass in a closure
// with our power
// ARCH: should this return decimals (which we don't support)?
func evalRound(cs ...*Chunk) (*Chunk, error) {
	var factor int
	if len(cs) == 2 {
		// TODO: check factor size (and test it)
		// what if this is not a literal? Do we want to round it to each value separately?
		factor = int(cs[1].storage.ints[0])
	}
	pow := math.Pow10(factor)
	switch cs[0].dtype {
	case DtypeInt:
		// cast to floats and do nothing (nothing happens, regardless of the factor specified)
		// ARCH: check how other engines behave, it would make sense to make it a noop (make sure
		// to edit return_types as well)
		return cs[0].cast(DtypeFloat)
	case DtypeFloat:
		if pow == 1 {
			return cs[0], nil
		}
		ctr := cs[0].Clone()
		for j, el := range ctr.storage.floats {
			// ARCH: is this the right way to round to n digits? What about overflows or loss of precision?
			// we can easily check by checking that abs(old-new) < 1
			ctr.storage.floats[j] = math.Round(pow*el) / pow
		}
		return ctr, nil
	default:
		return nil, fmt.Errorf("%w: round(%v)", errTypeNotSupported, cs[0].dtype)
	}
}

// this is essentially len(d) > utf8.RuneCount(d)
func hasRunes(d []byte) bool {
	for _, c := range d {
		if c >= utf8.RuneSelf {
			return true
		}
	}
	return false
}

// OPTIM: maybe if nchars > max(len(j)), we can just cheaply clone/return the existing column
// OPTIM: use nthValue returning []byte rather than string... and then use something cheaper than AddValue (AddValueNative?)
// ARCH: postgres allows for negative indexing
func evalLeft(cs ...*Chunk) (*Chunk, error) {
	nchars := int(cs[1].storage.ints[0])
	data := cs[0] // TODO(PR): redundant
	ret := NewChunk(DtypeString)

	runes := hasRunes(data.storage.strings)

	for j := 0; j < data.Len(); j++ {
		val := data.nthValue(j)
		if runes && utf8.RuneCountInString(val) > nchars {
			val = string([]rune(val)[:nchars])
		}
		if !runes && len(val) > nchars {
			val = val[:nchars]
		}

		if err := ret.AddValue(val); err != nil {
			return nil, err
		}
	}

	return ret, nil
}

func evalSplitPart(cs ...*Chunk) (*Chunk, error) {
	pos := int(cs[2].storage.ints[0])
	data := cs[0] // TODO(PR): redundant?
	needle := cs[1].nthValue(0)
	ret := NewChunk(DtypeString)

	for j := 0; j < data.Len(); j++ {
		val := data.nthValue(j)
		parts := strings.SplitN(val, needle, pos+1)
		if len(parts) == 1 || len(parts) < pos {
			val = ""
		} else {
			val = parts[pos-1]
		}

		if err := ret.AddValue(val); err != nil {
			return nil, err
		}
	}

	return ret, nil
}

func numFunc(fnc func(float64) float64) func(...*Chunk) (*Chunk, error) {
	return func(cs ...*Chunk) (*Chunk, error) {
		ct := cs[0]
		switch ct.dtype {
		case DtypeInt:
			rc, err := ct.cast(DtypeFloat)
			if err != nil {
				return nil, err
			}
			ctr := rc // TODO(PR): redundant?
			for j, el := range ctr.storage.floats {
				val := fnc(el)
				if math.IsNaN(val) || math.IsInf(val, 0) {
					if ctr.Nullability == nil {
						ctr.Nullability = bitmap.NewBitmap(ctr.Len())
					}
					ctr.Nullability.Set(j, true)
				}
				ctr.storage.floats[j] = val
			}
			return ctr, nil
		case DtypeFloat:
			ctr := ct.Clone()
			for j, el := range ctr.storage.floats {
				val := fnc(el)
				// ARCH: infinity is a valid float (well, so is nan), but I guess we cannot
				// get it as a legit value from an operation and it's a "placeholder" for some
				// weird operations - is that fair?
				// Also, note that if we allow for this, we'll have to deal with the JSON
				// serialisation issue
				if math.IsNaN(val) || math.IsInf(val, 0) {
					if ctr.Nullability == nil {
						ctr.Nullability = bitmap.NewBitmap(ctr.Len())
					}
					ctr.Nullability.Set(j, true)
				}
				ctr.storage.floats[j] = val
			}
			return ctr, nil
		default:
			return nil, fmt.Errorf("%w: func(%v)", errTypeNotSupported, ct.dtype)
		}
	}
}

func stringFunc(fnc func(string) string) func(...*Chunk) (*Chunk, error) {
	return func(cs ...*Chunk) (*Chunk, error) {
		ct := cs[0] // TODO(PR): redundant?
		if ct.IsLiteral {
			newValue := fnc(ct.nthValue(0))
			return NewChunkLiteralStrings(newValue, ct.Len()), nil
		}
		ret := NewChunk(DtypeString)
		if ct.Nullability != nil {
			ret.Nullability = ct.Nullability.Clone()
		}
		for j := 0; j < ct.Len(); j++ {
			newValue := fnc(ct.nthValue(j))
			if err := ret.AddValue(newValue); err != nil {
				return nil, err
			}
		}

		return ret, nil
	}
}

// TODO:
// date_part/date_trunc
// century
// day
// decade
// dow
// doy
// epoch
// hour
// isodow
// isoyear
// microseconds
// millennium
// milliseconds
// minute
// month
// quarter
// second
// timezone
// timezone_hour
// timezone_minute
// week
// year
