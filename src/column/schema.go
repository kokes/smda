package column

import (
	"errors"
	"fmt"
	"strconv"
)

type Dtype uint8

const (
	DtypeInvalid Dtype = iota
	DtypeNull
	DtypeString
	DtypeInt
	DtypeFloat
	DtypeBool
	// more to be added
	DtypeMax
)

func (dt Dtype) String() string {
	return []string{"invalid", "null", "string", "int", "float", "bool"}[dt]
}

// we want Dtypes to be marshaled within Schema correctly
// without this they'd be returned as an integer (even with ",string" tags)
func (dt Dtype) MarshalJSON() ([]byte, error) {
	retval := append([]byte{'"'}, []byte(dt.String())...)
	retval = append(retval, '"')
	return retval, nil
}

func (dt *Dtype) UnmarshalJSON(data []byte) error {
	if !(len(data) >= 2 && data[0] == '"' && data[len(data)-1] == '"') {
		return errors.New("unexpected string to be unmarshaled into a Dtype")
	}
	sdata := string(data[1 : len(data)-1])
	switch sdata {
	case "invalid":
		*dt = DtypeInvalid
	case "null":
		*dt = DtypeNull
	case "string":
		*dt = DtypeString
	case "int":
		*dt = DtypeInt
	case "float":
		*dt = DtypeFloat
	case "bool":
		*dt = DtypeBool
	default:
		return fmt.Errorf("unexpected type: %v", sdata)
	}

	return nil
}

type Schema struct {
	Name     string `json:"name"`
	Dtype    Dtype  `json:"dtype"`
	Nullable bool   `json:"nullable"`
}

func isNull(s string) bool {
	return s == "" // TODO: add custom null values as options (e.g. NA, N/A etc.)
}

// OPTIM: could we early exit by checking the input is all digits with a possible leading +-? are there any other constraints?
// basic microbenchmarks suggest a 2x speedup - thanks to being less general
// the one downside is that we'd be slower in the happy path (due to extra work) - so this would be beneficial if we have fewer ints than non-ints in general
func parseInt(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}

func parseFloat(s string) (float64, error) {
	return strconv.ParseFloat(s, 64)
}

// OPTIM: this seems slower than strconv.parseBool (it seemed before the refactor, retest now!)
// test it and maybe revert it, but be careful, parseBool parses 0/1 as bools (as it does True/False)
func parseBool(s string) (bool, error) {
	ln := len(s)

	switch ln {
	case 0:
		goto err
	case 1:
		if s == "t" || s == "T" {
			return true, nil
		}
		if s == "f" || s == "F" {
			return false, nil
		}
		goto err
	case 4:
		if s == "true" || s == "TRUE" {
			return true, nil
		}
		goto err
	case 5:
		if s == "false" || s == "FALSE" {
			return false, nil
		}
		goto err
	default:
		goto err
	}
err:
	return false, errors.New("not a bool")
}

// we need an early exit since parseInt and parseFloat are expensive
// does NOT cover infties, but we don't support them anyway (for now)
func containsDigit(s string) bool {
	for _, char := range s {
		if char >= '0' && char <= '9' {
			return true
		}
	}
	return false
}

// does NOT care about NULL inference, that's what isNull is for
// OPTIM: this function is weird, because it does allocate when benchmarking - but not when individual
// subfunctions are called - where are the allocations coming from? Improper inlining?
func guessType(s string) Dtype {
	// this is the fastest, so let's do this first
	if _, err := parseBool(s); err == nil {
		return DtypeBool
	}
	// early exit - only makes sense to do parse(Int|Float) if there's at least one digit
	if containsDigit(s) {
		if _, err := parseInt(s); err == nil {
			return DtypeInt
		}
		if _, err := parseFloat(s); err == nil {
			return DtypeFloat
		}
	}

	return DtypeString
}

type TypeGuesser struct {
	nullable bool
	types    map[Dtype]int
	nrows    int
}

func NewTypeGuesser() *TypeGuesser {
	return &TypeGuesser{
		nullable: false,
		types:    make(map[Dtype]int),
	}
}

// OPTIM: cost is 82, can be almost inlined (though it will get more expensive once isNull is fully defined)
func (tg *TypeGuesser) AddValue(s string) {
	tg.nrows++
	if s == "" {
		tg.nullable = true
		return
	}

	// OPTIM: we could use a slice instead of a map - it would improve insert performance, but the inferredType
	// logic would get more complicated - but it might be worthwhile - we run AddValue way more often
	tg.types[guessType(s)]++
}

func (tg *TypeGuesser) InferredType() Schema {
	if tg.nrows == 0 {
		return Schema{
			Dtype:    DtypeInvalid,
			Nullable: true, // nullability makes no sense here...?
		}
	}
	if len(tg.types) == 0 {
		return Schema{
			Dtype:    DtypeNull,
			Nullable: tg.nullable,
		}
	}

	if len(tg.types) == 1 {
		for key := range tg.types {
			return Schema{
				Dtype:    key,
				Nullable: tg.nullable,
			}
		}
	}

	// there are multiple guessed types, but they can all be numeric, so let's just settle
	// on a common type
	for g := range tg.types {
		if !(g == DtypeInt || g == DtypeFloat) {
			return Schema{
				Dtype:    DtypeString,
				Nullable: tg.nullable,
			}
		}
	}
	return Schema{
		Dtype:    DtypeFloat,
		Nullable: tg.nullable,
	}
}