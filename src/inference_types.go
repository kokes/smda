package smda

import (
	"errors"
	"fmt"
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

func parseBool(s string) (bool, error) {
	return strconv.ParseBool(s)
}

// does NOT care about NULL inference, that's what isNull is for
func guessType(s string) dtype {
	if _, err := parseInt(s); err == nil {
		return dtypeInt
	}
	if _, err := parseFloat(s); err == nil {
		return dtypeFloat
	}
	if _, err := parseBool(s); err == nil {
		return dtypeBool
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
