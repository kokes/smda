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
		"bool_tff": {"t", "f", "f"},
		"bool_ftf": {"f", "t", "f"},
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
