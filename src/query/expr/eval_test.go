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
		// doesn't work yet, because the data vector is different
		// so the answer is right, the verification code just doesn't understand it
		// {"nullif(foo123, 2)", column.DtypeInt, []string{"1", "", "3"}},
		// test nullifs with nulls, with literals, with other types
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
		"foo123":   {"1", "2", "3"},
		"bar134":   {"1", "3", "4"},
		"float123": {"1.0", "2.", "3"},
		"bool_tff": {"t", "f", "f"},
		"bool_ftf": {"f", "t", "f"},
		"str_foo":  {"f", "o", "o"},
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
