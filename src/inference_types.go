package smda

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
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
	bytes := []byte(s)
	// TODO: we're checking only the first 100 chars, but what is the maximum number of characters
	// a float can be represented in?
	if len(bytes) > 100 {
		bytes = bytes[:100]
	}
	for _, char := range bytes {
		if char >= '0' && char <= '9' {
			return true
		}
	}
	return false
}

// does NOT care about NULL inference, that's what isNull is for
// OPTIM: this function is weird, because it does allocate when benchmarking - but not when individual
// subfunctions are called - where are the allocations coming from? Improper inlining?
func guessType(s string) dtype {
	// this is the fastest, so let's do this first
	if _, err := parseBool(s); err == nil {
		return dtypeBool
	}
	// early exit - only makes sense to do parse(Int|Float) if there's at least one digit
	if containsDigit(s) {
		if _, err := parseInt(s); err == nil {
			return dtypeInt
		}
		if _, err := parseFloat(s); err == nil {
			return dtypeFloat
		}
	}

	return dtypeString
}

func (tg *typeGuesser) addValue(s string) {
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

func inferTypes(path string, settings loadSettings) ([]columnSchema, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	rl, err := newRawLoader(f, settings)
	if err != nil {
		return nil, err
	}

	row, err := rl.cr.Read()
	if err != nil {
		return nil, err // TODO: EOF handling?
	}
	hd := make([]string, len(row))
	copy(hd, row) // we're reusing records, so we need to copy here

	tgs := make([]*typeGuesser, 0, len(hd))
	for range hd {
		tgs = append(tgs, newTypeGuesser())
	}

	for {
		row, err := rl.cr.Read()
		// we don't want to trigger the internal ErrFieldCount,
		// we will handle column counts ourselves
		// TODO: we're duplicating this logic elsewhere (grep for `ErrFieldCount`)
		// maybe we should move this to the RawLoader
		if err != nil && err != csv.ErrFieldCount {
			// I think we need to report EOFs, because that will signalise to downstream
			// that no more stripe reads will be possible
			if err == io.EOF {
				break
			}
			return nil, err
		}
		for j, val := range row {
			tgs[j].addValue(val)
		}

	}
	ret := make([]columnSchema, len(tgs))
	for j, tg := range tgs {
		ret[j] = tg.inferredType()
		ret[j].Name = hd[j]
	}

	return ret, nil
}
