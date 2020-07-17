package expr

import (
	"reflect"
	"strings"
	"testing"

	"github.com/kokes/smda/src/column"
	"github.com/kokes/smda/src/database"
)

// TODO: test case insensitivity of keywords (just function names at this point) - it's not implemented yet
func TestIsValid(t *testing.T) {
	schema := database.TableSchema([]column.Schema{
		{Name: "my_int_column", Dtype: column.DtypeInt},
		{Name: "my_float_column", Dtype: column.DtypeFloat},
	})
	exprs := []string{
		"1 = 1", "1 != 1", "1 = 1.2", "1 > 0",
		"1 > my_int_column", "1.3 <= my_int_column",
		"(my_int_column > 3) = true", "(my_int_column > 3) = false",
	}

	for _, raw := range exprs {
		expr, err := ParseStringExpr(raw)
		if err != nil {
			t.Errorf("cannot parse %v, got %v", raw, err)
			continue
		}
		if err := expr.IsValid(schema); err != nil {
			t.Errorf("expecting %v to be a valid expression, got: %v", raw, err)
		}
	}
}

func TestIsValidNot(t *testing.T) {
	schema := database.TableSchema([]column.Schema{
		{Name: "my_int_column", Dtype: column.DtypeInt},
		{Name: "my_float_column", Dtype: column.DtypeFloat},
	})
	exprs := []string{
		"1 = 'bus'", "1 > 'foo'",
		"'bar' = my_int_column",
		// non-existing functions
	}

	for _, raw := range exprs {
		expr, err := ParseStringExpr(raw)
		if err != nil {
			t.Errorf("cannot parse %v, got %v", raw, err)
			continue
		}
		if expr.IsValid(schema) == nil {
			t.Errorf("expecting %v to be an invalid expression", raw)
		}
	}
}

func TestStringDedup(t *testing.T) {
	tests := []struct {
		input  string
		output string
	}{
		{"", ""},
		{"a", "a"},
		{"a,a", "a"},
		{"a,b,c", "a,b,c"},
		{"a,a,a,a,a,a,b,b,b,b", "a,b"},
		{"a,a,a,a,a,b", "a,b"},
		{"a,b,c,c,c,c,c,d,e,f,f,g", "a,b,c,d,e,f,g"},
	}

	for _, test := range tests {
		input := strings.Split(test.input, ",")
		output := strings.Split(test.output, ",")
		deduped := dedupeSortedStrings(input)
		if !reflect.DeepEqual(deduped, output) {
			t.Errorf("expecting %v to dedupe into %v, got %v instead", test.input, test.output, deduped)
		}
	}
}

func TestColumnsUsed(t *testing.T) {
	tests := []struct {
		rawExpr  string
		colsUsed []string
	}{
		{"1=1", nil},
		{"1=foo", []string{"foo"}},
		{"2*foo > bar-bak", []string{"bak", "bar", "foo"}},
		{"(2*foo > bar-bak) = true", []string{"bak", "bar", "foo"}},
		{"coalesce(a, b, c)", []string{"a", "b", "c"}},
		{"coalesce(a, c, b)", []string{"a", "b", "c"}}, // we return columns sorted
		{"coalesce(a, c, nullif(d, 4))", []string{"a", "c", "d"}},
		{"coalesce(a, c, 2*(1 - d))", []string{"a", "c", "d"}},

		{"a * a", []string{"a"}}, // dupes
		{"a * a / a", []string{"a"}},
		{"b * a / b", []string{"a", "b"}},
	}

	for _, test := range tests {
		expr, err := ParseStringExpr(test.rawExpr)
		if err != nil {
			t.Errorf("cannot parse %v, got %v", test.rawExpr, err)
			continue
		}
		used := expr.ColumnsUsed()
		if !reflect.DeepEqual(used, test.colsUsed) {
			t.Errorf("expecting %v to use %v, but got %v instead", test.rawExpr, test.colsUsed, used)
		}
	}
}

func TestReturnTypes(t *testing.T) {
	schema := database.TableSchema([]column.Schema{
		{Name: "my_int_column", Dtype: column.DtypeInt},
		{Name: "my_float_column", Dtype: column.DtypeFloat},
	})
	testCases := []struct {
		rawExpr    string
		returnType column.Schema
	}{
		// literals
		{"1", column.Schema{Dtype: column.DtypeInt}},
		{"1.23", column.Schema{Dtype: column.DtypeFloat}},
		{"true", column.Schema{Dtype: column.DtypeBool}},
		{"'ahoy'", column.Schema{Dtype: column.DtypeString}},
		{"my_int_column", column.Schema{Dtype: column.DtypeInt, Name: "my_int_column"}},

		// arithmetics
		{"1 = 1", column.Schema{Dtype: column.DtypeBool}},
		{"1 != 1", column.Schema{Dtype: column.DtypeBool}},
		{"1.2 > 1.3", column.Schema{Dtype: column.DtypeBool}},
		{"1.2 >= 1.3", column.Schema{Dtype: column.DtypeBool}},
		{"1.2 < 1.3", column.Schema{Dtype: column.DtypeBool}},
		{"1.2 <= 1.3", column.Schema{Dtype: column.DtypeBool}},
		{"my_float_column = 123", column.Schema{Dtype: column.DtypeBool}},
		{"my_float_column = my_int_column", column.Schema{Dtype: column.DtypeBool}},
		{"my_float_column <= my_int_column", column.Schema{Dtype: column.DtypeBool}},
		{"my_float_column > 12.234", column.Schema{Dtype: column.DtypeBool}},
		{"my_float_column > my_int_column", column.Schema{Dtype: column.DtypeBool}},

		{"1 + 2", column.Schema{Dtype: column.DtypeInt}},
		{"1 - 2", column.Schema{Dtype: column.DtypeInt}},
		{"1 * 2", column.Schema{Dtype: column.DtypeInt}},
		{"1 / 2", column.Schema{Dtype: column.DtypeFloat}},
		{"4 - my_int_column", column.Schema{Dtype: column.DtypeInt}},
		{"4 / my_int_column", column.Schema{Dtype: column.DtypeFloat}},
		{"my_float_column / my_int_column", column.Schema{Dtype: column.DtypeFloat}},

		// function calls
		{"count()", column.Schema{Dtype: column.DtypeInt}},
		{"count(my_int_column)", column.Schema{Dtype: column.DtypeInt}},
		{"nullif(my_int_column, 12)", column.Schema{Dtype: column.DtypeInt, Nullable: true}},
		{"nullif(my_float_column, 12)", column.Schema{Dtype: column.DtypeFloat, Nullable: true}},
		// {"NULLIF(my_float_column, 12)", column.Schema{Dtype: column.DtypeFloat, Nullable: true}}, // once we implement case folding...

		// "ahoy", "foo / bar", "2 * foo", "2+3*4", "count(foobar)", "bak = 'my literal'",
		// "coalesce(foo, bar, 1) - 4", "nullif(baz, 'foo')", "nullif(bak, 103)",
		// "round(1.234, 2)", "count(foo = true)", "bak != 3",
		// "sum(foo > 3)", "sum(foo < 3)", "sum(foo >= 3)", "sum(foo <= 3)",
	}

	for _, test := range testCases {
		expr, err := ParseStringExpr(test.rawExpr)
		if err != nil {
			t.Error(err)
			continue
		}

		retType, err := expr.ReturnType(schema)
		if err != nil {
			t.Error(err)
			continue
		}
		if !reflect.DeepEqual(retType, test.returnType) {
			t.Errorf("expecting %v to return a schema %v, got %v instead", test.rawExpr, test.returnType, retType)
		}

	}
}
