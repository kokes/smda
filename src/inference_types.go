package smda

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"unsafe"
)

type dtype uint8

const (
	dtypeInvalid dtype = iota
	dtypeNull
	dtypeString
	dtypeInt
	dtypeFloat
	dtypeBool
	// more to be added
	dtypeMax
)

func (dt dtype) String() string {
	return []string{"invalid", "null", "string", "int", "float", "bool"}[dt]
}

// we want dtypes to be marshaled within columnSchema correctly
// without this they'd be returned as an integer (even with ",string" tags)
func (dt dtype) MarshalJSON() ([]byte, error) {
	retval := append([]byte{'"'}, []byte(dt.String())...)
	retval = append(retval, '"')
	return retval, nil
}

func (dt *dtype) UnmarshalJSON(data []byte) error {
	if !(len(data) >= 2 && data[0] == '"' && data[len(data)-1] == '"') {
		return errors.New("unexpected string to be unmarshaled into a dtype")
	}
	sdata := string(data[1 : len(data)-1])
	switch sdata {
	case "invalid":
		*dt = dtypeInvalid
	case "null":
		*dt = dtypeNull
	case "string":
		*dt = dtypeString
	case "int":
		*dt = dtypeInt
	case "float":
		*dt = dtypeFloat
	case "bool":
		*dt = dtypeBool
	default:
		return fmt.Errorf("unexpected type: %v", sdata)
	}

	return nil
}

type typeGuesser struct {
	nullable bool
	types    map[dtype]int
	nrows    int
}

func newTypeGuesser() *typeGuesser {
	return &typeGuesser{
		nullable: false,
		types:    make(map[dtype]int),
	}
}

func isNull(b []byte) bool {
	return len(b) == 0 // TODO: add custom null values as options (e.g. NA, N/A etc.)
}

// OPTIM: could we early exit by checking the input is all digits with a possible leading +-? are there any other constraints?
// basic microbenchmarks suggest a 2x speedup - thanks to being less general
// the one downside is that we'd be slower in the happy path (due to extra work) - so this would be beneficial if we have fewer ints than non-ints in general
func parseInt(b []byte) (int64, error) {
	s := *(*string)(unsafe.Pointer(&b))
	return strconv.ParseInt(s, 10, 64)
}

func parseFloat(b []byte) (float64, error) {
	s := *(*string)(unsafe.Pointer(&b))
	return strconv.ParseFloat(s, 64)
}

// OPTIM: this seems slower than strconv.parseBool - test it and maybe revert it
// but be careful, parseBool parses 0/1 as bools (as it does True/False)
func parseBool(b []byte) (bool, error) {
	if bytes.Equal(b, []byte("t")) || bytes.Equal(b, []byte("T")) || bytes.Equal(b, []byte("true")) || bytes.Equal(b, []byte("TRUE")) {
		return true, nil
	}
	if bytes.Equal(b, []byte("f")) || bytes.Equal(b, []byte("F")) || bytes.Equal(b, []byte("false")) || bytes.Equal(b, []byte("FALSE")) {
		return false, nil
	}

	return false, errors.New("not a bool")
}

// we need an early exit since parseInt and parseFloat are expensive
// does NOT cover infties, but we don't support them anyway (for now)
func containsDigit(b []byte) bool {
	// TODO: we're checking only the first 100 chars, but what is the maximum number of characters
	// a float can be represented in?
	if len(b) > 100 {
		b = b[:100]
	}
	for _, char := range b {
		if char >= '0' && char <= '9' {
			return true
		}
	}
	return false
}

// does NOT care about NULL inference, that's what isNull is for
// OPTIM: this function is weird, because it does allocate when benchmarking - but not when individual
// subfunctions are called - where are the allocations coming from? Improper inlining?
func guessType(b []byte) dtype {
	// this is the fastest, so let's do this first
	if _, err := parseBool(b); err == nil {
		return dtypeBool
	}
	// early exit - only makes sense to do parse(Int|Float) if there's at least one digit
	if containsDigit(b) {
		if _, err := parseInt(b); err == nil {
			return dtypeInt
		}
		if _, err := parseFloat(b); err == nil {
			return dtypeFloat
		}
	}

	return dtypeString
}

func (tg *typeGuesser) addValue(s []byte) {
	tg.nrows++
	if isNull(s) {
		tg.nullable = true
		return
	}

	tg.types[guessType(s)]++
}

func (tg *typeGuesser) inferredType() columnSchema {
	if tg.nrows == 0 {
		return columnSchema{
			Dtype:    dtypeInvalid,
			Nullable: true, // nullability makes no sense here...?
		}
	}
	if len(tg.types) == 0 {
		return columnSchema{
			Dtype:    dtypeNull,
			Nullable: tg.nullable,
		}
	}

	if len(tg.types) == 1 {
		for key := range tg.types {
			return columnSchema{
				Dtype:    key,
				Nullable: tg.nullable,
			}
		}
	}

	// there are multiple guessed types, but they can all be numeric, so let's just settle
	// on a common type
	for g := range tg.types {
		if !(g == dtypeInt || g == dtypeFloat) {
			return columnSchema{
				Dtype:    dtypeString,
				Nullable: tg.nullable,
			}
		}
	}
	return columnSchema{
		Dtype:    dtypeFloat,
		Nullable: tg.nullable,
	}
}

func (db *Database) inferTypes(ds *Dataset) ([]columnSchema, error) {
	for _, col := range ds.Schema {
		if col.Dtype != dtypeString {
			return nil, errors.New("can only infer types from strings")
		}
		if col.Nullable {
			return nil, errors.New("can only infer from non-nullable columns")
		}
	}

	ret := make([]columnSchema, 0, len(ds.Schema))

	// there are now two ways of inference order - either we load column by column, which
	// will be more memory efficient, because we'll need to keep metadata in memory for only one
	// column at a time (and could be useful in the future, if we track unique values etc.)
	// or we go stripe by stripe, which will be more file IO efficient
	// let's do column by column for now, because we don't have efficient stripe reading anyway
	for colNum, col := range ds.Schema {
		// log.Println("Processing", col.Name)
		tg := newTypeGuesser()
		for _, stripeID := range ds.Stripes {
			chunk, err := db.readColumnFromStripe(ds, stripeID, colNum)
			if err != nil {
				return nil, err
			}
			schunk, ok := chunk.(*columnStrings)
			if !ok {
				return nil, errors.New("unexpected type error")
			}
			// OPTIM: in many cases we already know we can't have all ints/floats/bools, so it doesn't make sense
			// to check types any more - it's only useful for reporting - will we use it for that ever?
			// there's one sad reality - we can't quite do this easily, because we will lose information about nullability
			// - that is, if we break on first inference of a string, we won't know if it's a nullable string or not
			// What we could do: test for nullability (fast), but only test for types if we haven't settled on strings yet
			for j := 0; j < schunk.Len(); j++ {
				tg.addValue(schunk.nthValue(j))
			}
		}
		// we infer a type from the given column, but we don't know the name,
		// so we need to get it from the original, string-only, schema
		itype := tg.inferredType()
		itype.Name = col.Name
		ret = append(ret, itype)
	}

	return ret, nil
}
