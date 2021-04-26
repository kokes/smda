package expr

import (
	"testing"

	"github.com/kokes/smda/src/column"
	"github.com/kokes/smda/src/database"
)

// TODO(prio): consider moving this to query_test, so that we test return_types evaluation etc.
func TestBasicEval(t *testing.T) {
	tests := []struct {
		expr        string
		outputDtype column.Dtype
		outputData  []string
	}{
		{"foo123", column.DtypeInt, []string{"1", "2", "3"}},
		{"foo123 = bar134", column.DtypeBool, []string{"t", "f", "f"}},
		{"foo123 != bar134", column.DtypeBool, []string{"f", "t", "t"}},
		{"foo123 > bar134", column.DtypeBool, []string{"f", "f", "f"}},
		{"foo123 >= bar134", column.DtypeBool, []string{"t", "f", "f"}},
		{"foo123 < bar134", column.DtypeBool, []string{"f", "t", "t"}},
		{"foo123 <= bar134", column.DtypeBool, []string{"t", "t", "t"}},
		{"bool_tff OR bool_ftf", column.DtypeBool, []string{"t", "t", "f"}},
		{"bool_tff or bool_ftf", column.DtypeBool, []string{"t", "t", "f"}},
		{"bool_tff AND bool_ftf", column.DtypeBool, []string{"f", "f", "f"}},
		{"bool_tff and bool_ftf", column.DtypeBool, []string{"f", "f", "f"}},

		// literals
		{"foo123 > 1", column.DtypeBool, []string{"f", "t", "t"}},
		{"foo123 >= 1", column.DtypeBool, []string{"t", "t", "t"}},
		{"1 < foo123", column.DtypeBool, []string{"f", "t", "t"}},
		{"2 >= foo123", column.DtypeBool, []string{"t", "t", "f"}},

		{"float123 > 1.1", column.DtypeBool, []string{"f", "t", "t"}},
		{"float123 >= 0.9", column.DtypeBool, []string{"t", "t", "t"}},
		{"1.1 < float123", column.DtypeBool, []string{"f", "t", "t"}},
		{"2.2 >= float123", column.DtypeBool, []string{"t", "t", "f"}},

		// infix operators
		// TODO(PR): make this work
		// {"not bool_tff", column.DtypeBool, []string{"f", "t", "t"}},
		// {"-float123", column.DtypeFloat, []string{"-1", "-2", "-3"}},
		// {"-foo123", column.DtypeInt, []string{"-1", "-2", "-3"}},

		{"bool_tff = true", column.DtypeBool, []string{"t", "f", "f"}},
		{"bool_tff != true", column.DtypeBool, []string{"f", "t", "t"}},
		{"bool_tff = false", column.DtypeBool, []string{"f", "t", "t"}},
		{"bool_tff >= false", column.DtypeBool, []string{"t", "t", "t"}},
		{"bool_tff > false", column.DtypeBool, []string{"t", "f", "f"}},
		{"false = bool_tff", column.DtypeBool, []string{"f", "t", "t"}},
		{"false <= bool_tff", column.DtypeBool, []string{"t", "t", "t"}},
		{"false < bool_tff", column.DtypeBool, []string{"t", "f", "f"}},

		{"str_foo >= str_foo", column.DtypeBool, []string{"t", "t", "t"}},
		{"str_foo != str_foo", column.DtypeBool, []string{"f", "f", "f"}},
		{"str_foo = 'o'", column.DtypeBool, []string{"f", "t", "t"}},
		{"str_foo != 'f'", column.DtypeBool, []string{"f", "t", "t"}},

		// all literals
		{"(foo123 > 0) AND (2 >= 1)", column.DtypeBool, []string{"t", "t", "t"}},
		// TODO(next): make this work
		// {"4 > 1", column.DtypeBool, []string{"lit:t"}},

		// functions
		{"nullif(foo123, 5)", column.DtypeInt, []string{"1", "2", "3"}},
		// the underlying []int64 doesn't change, but ChunksEqual doesn't compare those, it looks at the "real" values
		{"nullif(foo123, 2)", column.DtypeInt, []string{"1", "", "3"}},
		// test nullifs with nulls, with literals, with other types

		{"round(float123, 2)", column.DtypeFloat, []string{"1", "2", "3"}},
		{"round(foo123, 0)", column.DtypeFloat, []string{"1", "2", "3"}},
		{"round(float123)", column.DtypeFloat, []string{"1", "2", "3"}},
		{"round(foo123)", column.DtypeFloat, []string{"1", "2", "3"}},
		{"round(foo123, 2)", column.DtypeFloat, []string{"1", "2", "3"}},
		// {"round(2.234, 2)", column.DtypeFloat, []string{"2.23"}}, // don't have a way of testing literals just yet
		{"round(float1p452p13p0, 1)", column.DtypeFloat, []string{"1.5", "2.1", "3.0"}},
		// don't have a good way to specify floats precisely (though check out log(float123)), so let's just test approx values
		{"round(sin(float123), 4)", column.DtypeFloat, []string{"0.8415", "0.9093", "0.1411"}},
		{"round(sin(foo123), 4)", column.DtypeFloat, []string{"0.8415", "0.9093", "0.1411"}},
		{"exp2(float123)", column.DtypeFloat, []string{"2", "4", "8"}},
		{"exp2(floatneg123)", column.DtypeFloat, []string{"0.5", "0.25", "0.125"}},
		{"log(float123)", column.DtypeFloat, []string{"0", "0.6931471805599453", "1.0986122886681096"}},
		{"log(foo123)", column.DtypeFloat, []string{"0", "0.6931471805599453", "1.0986122886681096"}},
		{"log2(float123)", column.DtypeFloat, []string{"0", "1", "1.5849625007211563"}},
		{"log2(foo123)", column.DtypeFloat, []string{"0", "1", "1.5849625007211563"}},
		{"log10(float123)", column.DtypeFloat, []string{"0", "0.3010299956639812", "0.4771212547196624"}},
		{"log(floatneg123)", column.DtypeFloat, []string{"", "", ""}},
		// string functions
		// TODO(next): test literals
		{"trim(names_ws)", column.DtypeString, []string{"joe", "jane", "bob"}},
		{"lower(names)", column.DtypeString, []string{"joe", "ondřej", "bob"}},
		{"upper(names)", column.DtypeString, []string{"JOE", "ONDŘEJ", "BOB"}},
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

	ds, err := db.LoadDatasetFromMap(map[string][]string{
		"foo123":          {"1", "2", "3"},
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
		res, err := Evaluate(expr, len(test.outputData), coldata, nil)
		if err != nil {
			t.Error(err)
			continue
		}
		expected := column.NewChunkFromSchema(column.Schema{Dtype: test.outputDtype})
		if err := expected.AddValues(test.outputData); err != nil {
			t.Error(err)
			continue
		}
		if !column.ChunksEqual(res, expected) {
			t.Errorf("expected expression %+v to result in\n\t%+v, got\n\t%+v instead", test.expr, expected, res)
		}
	}
}

// UpdateAggregator
// TODO(next): eval with filters? or leave that to query?
