package expr

import (
	"testing"

	"github.com/kokes/smda/src/column"
	"github.com/kokes/smda/src/database"
)

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
		{"bool_tff || bool_ftf", column.DtypeBool, []string{"t", "t", "f"}},
		{"bool_tff && bool_ftf", column.DtypeBool, []string{"f", "f", "f"}},

		// literals
		{"foo123 > 1", column.DtypeBool, []string{"f", "t", "t"}},
		{"foo123 >= 1", column.DtypeBool, []string{"t", "t", "t"}},
		{"1 < foo123", column.DtypeBool, []string{"f", "t", "t"}},
		{"2 >= foo123", column.DtypeBool, []string{"t", "t", "f"}},

		{"float123 > 1.1", column.DtypeBool, []string{"f", "t", "t"}},
		{"float123 >= 0.9", column.DtypeBool, []string{"t", "t", "t"}},
		{"1.1 < float123", column.DtypeBool, []string{"f", "t", "t"}},
		{"2.2 >= float123", column.DtypeBool, []string{"t", "t", "f"}},

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
		// doesn't work yet, because we don't have stripe length info in our expression evaluator
		// {"(foo123 > 0) && (2 >= 1)", column.DtypeBool, []string{"t", "t", "t"}},

		// functions
		{"nullif(foo123, 5)", column.DtypeInt, []string{"1", "2", "3"}},
		// the underlying []int64 doesn't change, but ChunksEqual doesn't compare those, it looks at the "real" values
		{"nullif(foo123, 2)", column.DtypeInt, []string{"1", "", "3"}},
		// test nullifs with nulls, with literals, with other types

		{"round(float123, 2)", column.DtypeFloat, []string{"1", "2", "3"}},
		// {"round(2.234, 2)", column.DtypeFloat, []string{"2.23"}}, // don't have a way of testing literals just yet
		{"round(float1p452p13p0, 1)", column.DtypeFloat, []string{"1.5", "2.1", "3.0"}},
		// don't have a good way to specify floats precisely (though check out log(float123)), so let's just test approx values
		{"round(sin(float123), 4)", column.DtypeFloat, []string{"0.8415", "0.9093", "0.1411"}},
		// {"round(sin(foo123), 4)", column.DtypeFloat, []string{"0.8415", "0.9093", "0.1411"}}, // int func calls not implemented yet
		{"exp2(float123)", column.DtypeFloat, []string{"2", "4", "8"}},
		{"exp2(floatneg123)", column.DtypeFloat, []string{"0.5", "0.25", "0.125"}},
		{"log(float123)", column.DtypeFloat, []string{"0", "0.6931471805599453", "1.0986122886681096"}},
		{"log2(float123)", column.DtypeFloat, []string{"0", "1", "1.5849625007211563"}},
		{"log10(float123)", column.DtypeFloat, []string{"0", "0.3010299956639812", "0.4771212547196624"}},
		// test negatives in floats (-> nulls, not nans) [not implemented yet]
		// {"log(floatneg123)", column.DtypeFloat, []string{"", "", ""}},
	}

	db, err := database.NewDatabase(nil)
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
	})
	if err != nil {
		t.Fatal(err)
	}
	coldata := make(map[string]column.Chunk)
	for _, cln := range ds.Schema {
		column, err := db.ReadColumnFromStripeByName(ds, ds.Stripes[0], cln.Name)
		if err != nil {
			t.Fatal(err)
		}
		coldata[cln.Name] = column
	}

	for _, test := range tests {
		expr, err := ParseStringExpr(test.expr)
		if err != nil {
			t.Error(err)
			continue
		}
		res, err := Evaluate(expr, coldata)
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
			t.Errorf("expected expression %v to result in %v, got %v instead", test.expr, expected, res)
		}
	}
}

// UpdateAggregator
