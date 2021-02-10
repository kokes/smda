package column

import (
	"errors"
	"fmt"
	"strconv"
)

// Dtype denotes the data type of a given object (e.g. int or string)
type Dtype uint8

// individual dtypes defined as a sequence
const (
	DtypeInvalid Dtype = iota
	DtypeNull
	DtypeString
	DtypeInt
	DtypeFloat
	DtypeBool
	DtypeDate
	DtypeDatetime
	// more to be added
	DtypeMax
)

func (dt Dtype) String() string {
	return []string{"invalid", "null", "string", "int", "float", "bool", "date", "datetime"}[dt]
}

// MarshalJSON returns the JSON representation of a dtype (stringified + json string)
// we want Dtypes to be marshaled within Schema correctly
// without this they'd be returned as an integer (even with ",string" tags)
func (dt Dtype) MarshalJSON() ([]byte, error) {
	retval := append([]byte{'"'}, []byte(dt.String())...)
	retval = append(retval, '"')
	return retval, nil
}

// UnmarshalJSON deserialises a given dtype from a JSON value
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
	case "date":
		*dt = DtypeDate
	case "datetime":
		*dt = DtypeDatetime
	default:
		return fmt.Errorf("unexpected type: %v", sdata)
	}

	return nil
}

// Schema defines all the necessary properties of column
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

// OPTIM: for some reason, this allocates... but not on its own, only when combined with other detectors
// in guessType. I couldn't quite figure out why.
// I found out why! The error reporting in all strconv functions... allocates
// we could copy paste the code and just remove the errors (and have a bool function) or we could
// take the code from `go/scanner`: `func (s *Scanner) scanNumber() (token.Token, string) {`
// Also, extracting strconv.ParseInt and removing the error reporting dropped the sad path from 50ns to 14ns per op
// all in all, this seems to speed up type detection, but it won't help much with the overall upload perf (seemingly)
// probably because that pipeline is dominated by the whole chunk construction bit
// FloatDetection               117MB/s ± 2%    230MB/s ± 1%   +95.82%  (p=0.000 n=6+10)
// BoolDetection                914MB/s ± 2%    886MB/s ± 1%    -3.02%  (p=0.000 n=6+9)
// StringDetection             44.7MB/s ± 1%  139.6MB/s ± 1%  +212.60%  (p=0.000 n=6+10)
// IntDetection                 195MB/s ± 1%    210MB/s ± 1%    +7.62%  (p=0.000 n=6+10)
// DateDetection               54.2MB/s ± 1%  106.5MB/s ± 1%   +96.63%  (p=0.000 n=6+10)
// DatetimeDetection            120MB/s ± 0%    213MB/s ± 0%   +77.33%  (p=0.002 n=5+8)
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

// does NOT care about NULL inference, that's what isNull is for
func guessType(s string) Dtype {
	// this is the fastest, so let's do this first
	if _, err := parseBool(s); err == nil {
		return DtypeBool
	}
	if _, err := parseInt(s); err == nil {
		return DtypeInt
	}
	if _, err := parseFloat(s); err == nil {
		return DtypeFloat
	}
	if _, err := parseDate(s); err == nil {
		return DtypeDate
	}
	if _, err := parseDatetime(s); err == nil {
		return DtypeDatetime
	}

	return DtypeString
}

// TypeGuesser contains state necessary for inferring types from a stream of strings
type TypeGuesser struct {
	nullable bool
	types    [DtypeMax]int
	nrows    int
}

// NewTypeGuesser creates a new type guesser
func NewTypeGuesser() *TypeGuesser {
	return &TypeGuesser{}
}

// AddValue feeds a new value to a type guesser
func (tg *TypeGuesser) AddValue(s string) {
	tg.nrows++
	if isNull(s) {
		tg.nullable = true
		return
	}
	// if we once detected a string, we cannot overturn this
	if tg.types[DtypeString] > 0 {
		return
	}

	tg.types[guessType(s)]++
}

// InferredType returns the best guess of a type for a given stream of strings
func (tg *TypeGuesser) InferredType() Schema {
	if tg.nrows == 0 {
		return Schema{
			Dtype:    DtypeInvalid,
			Nullable: true, // nullability makes no sense here...?
		}
	}
	tgmap := make(map[Dtype]int)
	for j, val := range tg.types {
		if val > 0 {
			tgmap[Dtype(j)] = val
		}
	}
	if len(tgmap) == 0 {
		return Schema{
			Dtype:    DtypeNull,
			Nullable: tg.nullable,
		}
	}

	if len(tgmap) == 1 {
		for key := range tgmap {
			return Schema{
				Dtype:    key,
				Nullable: tg.nullable,
			}
		}
	}

	// there are multiple guessed types, but they can all be numeric, so let's just settle
	// on a common type
	for g := range tgmap {
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
