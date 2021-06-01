package expr

import (
	"errors"
	"strings"
	"testing"

	"github.com/kokes/smda/src/column"
	"github.com/kokes/smda/src/database"
)

var litPrefix = "lit:"

func prepColumn(nrows int, dtype column.Dtype, rawData string) (column.Chunk, error) {
	c := column.NewChunkFromSchema(column.Schema{Dtype: dtype})
	var err error
	if strings.HasPrefix(rawData, litPrefix) {
		c, err = column.NewChunkLiteralTyped(strings.TrimPrefix(rawData, litPrefix), dtype, nrows)
		if err != nil {
			return nil, err
		}
	} else {
		if err := c.AddValues(strings.Split(rawData, ",")); err != nil {
			return nil, err
		}
	}
	return c, err
}

// TODO(prio): consider moving this to query_test, so that we test return_types evaluation etc.
func TestBasicEval(t *testing.T) {
	tests := []struct {
		expr         string
		outputDtype  column.Dtype
		outputLength int
		outputData   string
		err          error
	}{
		{"foo123", column.DtypeInt, 3, "1,2,3", nil},
		{"foo123 = bar134", column.DtypeBool, 3, "t,f,f", nil},
		{"foo123 != bar134", column.DtypeBool, 3, "f,t,t", nil},
		{"foo123 > bar134", column.DtypeBool, 3, "f,f,f", nil},
		{"foo123 >= bar134", column.DtypeBool, 3, "t,f,f", nil},
		{"foo123 < bar134", column.DtypeBool, 3, "f,t,t", nil},
		{"foo123 <= bar134", column.DtypeBool, 3, "t,t,t", nil},
		{"bool_tff OR bool_ftf", column.DtypeBool, 3, "t,t,f", nil},
		{"bool_tff or bool_ftf", column.DtypeBool, 3, "t,t,f", nil},
		{"bool_tff AND bool_ftf", column.DtypeBool, 3, "f,f,f", nil},
		{"bool_tff and bool_ftf", column.DtypeBool, 3, "f,f,f", nil},

		// TODO(next): basic arithmetics
		{"foo123 / foo123", column.DtypeFloat, 3, "1,1,1", nil},

		// division by zero
		{"foo123 / foo120", column.DtypeFloat, 3, "", errDivisionByZero},
		{"foo123 / (foo123-2)", column.DtypeFloat, 3, "", errDivisionByZero},

		// literals
		{"foo123 > 1", column.DtypeBool, 3, "f,t,t", nil},
		{"foo123 >= 1", column.DtypeBool, 3, "t,t,t", nil},
		{"1 < foo123", column.DtypeBool, 3, "f,t,t", nil},
		{"2 >= foo123", column.DtypeBool, 3, "t,t,f", nil},

		{"float123 > 1.1", column.DtypeBool, 3, "f,t,t", nil},
		{"float123 >= 0.9", column.DtypeBool, 3, "t,t,t", nil},
		{"1.1 < float123", column.DtypeBool, 3, "f,t,t", nil},
		{"2.2 >= float123", column.DtypeBool, 3, "t,t,f", nil},

		// prefix operators
		{"not bool_tff", column.DtypeBool, 3, "f,t,t", nil},
		{"not (not bool_tff)", column.DtypeBool, 3, "t,f,f", nil},
		{"-float123", column.DtypeFloat, 3, "-1,-2,-3", nil},
		{"-foo123", column.DtypeInt, 3, "-1,-2,-3", nil},
		{"+foo123", column.DtypeInt, 3, "1,2,3", nil},

		// infix operators
		{"bool_tff = true", column.DtypeBool, 3, "t,f,f", nil},
		{"bool_tff != true", column.DtypeBool, 3, "f,t,t", nil},
		{"bool_tff = false", column.DtypeBool, 3, "f,t,t", nil},
		{"bool_tff >= false", column.DtypeBool, 3, "t,t,t", nil},
		{"bool_tff > false", column.DtypeBool, 3, "t,f,f", nil},
		{"false = bool_tff", column.DtypeBool, 3, "f,t,t", nil},
		{"false <= bool_tff", column.DtypeBool, 3, "t,t,t", nil},
		{"false < bool_tff", column.DtypeBool, 3, "t,f,f", nil},

		{"str_foo >= str_foo", column.DtypeBool, 3, "t,t,t", nil},
		{"str_foo != str_foo", column.DtypeBool, 3, "f,f,f", nil},
		{"str_foo = 'o'", column.DtypeBool, 3, "f,t,t", nil},
		{"str_foo != 'f'", column.DtypeBool, 3, "f,t,t", nil},

		// all literals
		{"(foo123 > 0) AND (2 >= 1)", column.DtypeBool, 3, "t,t,t", nil},
		{"4 > 1", column.DtypeBool, 3, "lit:t", nil},
		{"4 < 1", column.DtypeBool, 3, "lit:f", nil},

		// functions
		{"nullif(foo123, 5)", column.DtypeInt, 3, "1,2,3", nil},
		// the underlying []int64 doesn't change, but ChunksEqual doesn't compare those, it looks at the "real" values
		{"nullif(foo123, 2)", column.DtypeInt, 3, "1,,3", nil},
		// test nullifs with nulls, with literals, with other types

		{"round(float123, 2)", column.DtypeFloat, 3, "1,2,3", nil},
		{"round(foo123, 0)", column.DtypeFloat, 3, "1,2,3", nil},
		{"round(float123)", column.DtypeFloat, 3, "1,2,3", nil},
		{"round(foo123)", column.DtypeFloat, 3, "1,2,3", nil},
		{"round(foo123, 2)", column.DtypeFloat, 3, "1,2,3", nil},
		{"round(2.234, 2)", column.DtypeFloat, 3, "lit:2.23", nil},
		{"round(float1p452p13p0, 1)", column.DtypeFloat, 3, "1.5,2.1,3.0", nil},
		// don't have a good way to specify floats precisely (though check out log(float123)), so let's just test approx values
		{"round(sin(float123), 4)", column.DtypeFloat, 3, "0.8415,0.9093,0.1411", nil},
		{"round(sin(foo123), 4)", column.DtypeFloat, 3, "0.8415,0.9093,0.1411", nil},
		{"exp2(float123)", column.DtypeFloat, 3, "2,4,8", nil},
		{"exp2(floatneg123)", column.DtypeFloat, 3, "0.5,0.25,0.125", nil},
		{"log(float123)", column.DtypeFloat, 3, "0,0.6931471805599453,1.0986122886681096", nil},
		{"log(foo123)", column.DtypeFloat, 3, "0,0.6931471805599453,1.0986122886681096", nil},
		{"log2(float123)", column.DtypeFloat, 3, "0,1,1.5849625007211563", nil},
		{"log2(foo123)", column.DtypeFloat, 3, "0,1,1.5849625007211563", nil},
		{"log10(float123)", column.DtypeFloat, 3, "0,0.3010299956639812,0.4771212547196624", nil},
		{"log(floatneg123)", column.DtypeFloat, 3, ",,", nil},
		// string functions
		{"trim(names_ws)", column.DtypeString, 3, "joe,jane,bob", nil},
		{"lower(names)", column.DtypeString, 3, "joe,ondřej,bob", nil},
		{"lower('')", column.DtypeString, 3, "lit:", nil},
		{"upper('')", column.DtypeString, 3, "lit:", nil},
		{"lower('ŘEJ')", column.DtypeString, 3, "lit:řej", nil},
		{"lower('JOE')", column.DtypeString, 3, "lit:joe", nil},
		{"upper(names)", column.DtypeString, 3, "JOE,ONDŘEJ,BOB", nil},
		{"upper('joe')", column.DtypeString, 3, "lit:JOE", nil},
		{"upper('joE')", column.DtypeString, 3, "lit:JOE", nil},
		{"upper('Ondřej')", column.DtypeString, 3, "lit:ONDŘEJ", nil},
		{"upper('řej')", column.DtypeString, 3, "lit:ŘEJ", nil},
		{"trim(' foo ')", column.DtypeString, 3, "lit:foo", nil},
		{"trim('')", column.DtypeString, 3, "lit:", nil},
		{"trim('	')", column.DtypeString, 3, "lit:", nil},
		{"left(names, 2)", column.DtypeString, 3, "Jo,On,Bo", nil},
		{"left(names, 100)", column.DtypeString, 3, "Joe,Ondřej,Bob", nil},
		{"left(names, 4)", column.DtypeString, 3, "Joe,Ondř,Bob", nil}, // testing multi-byte characters
		{"left(names, 1)", column.DtypeString, 3, "J,O,B", nil},
		{"left(names, 0)", column.DtypeString, 3, ",,", nil},
		{"split_part(names, 'o', 1)", column.DtypeString, 3, "J,,B", nil},
		{"split_part(names, 'o', 2)", column.DtypeString, 3, "e,,b", nil},
		{"split_part(names, 'o', 3)", column.DtypeString, 3, ",,", nil},
	}

	db, err := database.NewDatabase("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()

	ds, err := db.LoadDatasetFromMap("dataset", map[string][]string{
		"foo123":          {"1", "2", "3"},
		"foo120":          {"1", "2", "0"},
		"bar134":          {"1", "3", "4"},
		"float123":        {"1.0", "2.", "3"},
		"floatneg123":     {"-1.0", "-2.", "-3"},
		"float1p452p13p0": {"1.45", "2.1", "3.0"},
		"bool_tff":        {"t", "f", "f"},
		"bool_ftf":        {"f", "t", "f"},
		"str_foo":         {"f", "o", "o"},
		"names":           {"Joe", "Ondřej", "Bob"},
		"names_ws": {"		joe ", "jane	", " bob "},
	})
	if err != nil {
		t.Fatal(err)
	}
	columns := make([]string, 0, len(ds.Schema))
	for _, cln := range ds.Schema {
		columns = append(columns, cln.Name)
	}
	coldata, err := db.ReadColumnsFromStripeByNames(ds, ds.Stripes[0], columns)
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range tests {
		expr, err := ParseStringExpr(test.expr)
		if err != nil {
			t.Error(err)
			continue
		}
		// ARCH: we don't have chunk length explicitly, so we're just setting it to the length of our expected output
		res, err := Evaluate(expr, test.outputLength, coldata, nil)
		if !errors.Is(err, test.err) {
			t.Errorf("expecting %v to result in err %v, got %v instead", test.expr, test.err, err)
			continue
		}
		if test.err != nil {
			continue
		}
		expected, err := prepColumn(test.outputLength, test.outputDtype, test.outputData)
		if err != nil {
			t.Error(err)
			continue
		}
		if !column.ChunksEqual(res, expected) {
			t.Errorf("expected expression %+v to result in\n\t%+v, got\n\t%+v instead", test.expr, expected, res)
		}
	}
}

// UpdateAggregator
