package expr

import (
	"reflect"
	"testing"

	"github.com/kokes/smda/src/database"
)

// TODO: test case insensitivity of keywords (just function names at this point) - it's not implemented yet
func TestIsValid(t *testing.T) {
	schema := database.TableSchema([]database.ColumnSchema{
		{Name: "my_int_column", Dtype: database.DtypeInt},
		{Name: "my_float_column", Dtype: database.DtypeFloat},
	})
	exprs := []string{
		"1 = 1", "1 != 1", "1 = 1.2", "1 > 0",
		"1 > my_int_column", "1.3 <= my_int_column",
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
	schema := database.TableSchema([]database.ColumnSchema{
		{Name: "my_int_column", Dtype: database.DtypeInt},
		{Name: "my_float_column", Dtype: database.DtypeFloat},
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

func TestReturnTypes(t *testing.T) {
	schema := database.TableSchema([]database.ColumnSchema{
		{Name: "my_int_column", Dtype: database.DtypeInt},
		{Name: "my_float_column", Dtype: database.DtypeFloat},
	})
	testCases := []struct {
		rawExpr    string
		returnType database.ColumnSchema
	}{
		// literals
		{"1", database.ColumnSchema{Dtype: database.DtypeInt}},
		{"1.23", database.ColumnSchema{Dtype: database.DtypeFloat}},
		// {"true", database.ColumnSchema{Dtype: database.DtypeBool}},
		{"'ahoy'", database.ColumnSchema{Dtype: database.DtypeString}},
		{"my_int_column", database.ColumnSchema{Dtype: database.DtypeInt, Name: "my_int_column"}},

		// arithmetics
		{"1 = 1", database.ColumnSchema{Dtype: database.DtypeBool}},
		{"1 != 1", database.ColumnSchema{Dtype: database.DtypeBool}},
		{"1.2 > 1.3", database.ColumnSchema{Dtype: database.DtypeBool}},
		{"1.2 >= 1.3", database.ColumnSchema{Dtype: database.DtypeBool}},
		{"1.2 < 1.3", database.ColumnSchema{Dtype: database.DtypeBool}},
		{"1.2 <= 1.3", database.ColumnSchema{Dtype: database.DtypeBool}},
		{"my_float_column = 123", database.ColumnSchema{Dtype: database.DtypeBool}},
		{"my_float_column = my_int_column", database.ColumnSchema{Dtype: database.DtypeBool}},
		{"my_float_column <= my_int_column", database.ColumnSchema{Dtype: database.DtypeBool}},
		{"my_float_column > 12.234", database.ColumnSchema{Dtype: database.DtypeBool}},
		{"my_float_column > my_int_column", database.ColumnSchema{Dtype: database.DtypeBool}},

		{"1 + 2", database.ColumnSchema{Dtype: database.DtypeInt}},
		{"1 - 2", database.ColumnSchema{Dtype: database.DtypeInt}},
		{"1 * 2", database.ColumnSchema{Dtype: database.DtypeInt}},
		{"1 / 2", database.ColumnSchema{Dtype: database.DtypeFloat}},
		{"4 - my_int_column", database.ColumnSchema{Dtype: database.DtypeInt}},
		{"4 / my_int_column", database.ColumnSchema{Dtype: database.DtypeFloat}},
		{"my_float_column / my_int_column", database.ColumnSchema{Dtype: database.DtypeFloat}},

		// function calls
		{"count()", database.ColumnSchema{Dtype: database.DtypeInt}},
		{"count(my_int_column)", database.ColumnSchema{Dtype: database.DtypeInt}},
		{"nullif(my_int_column, 12)", database.ColumnSchema{Dtype: database.DtypeInt, Nullable: true}},
		{"nullif(my_float_column, 12)", database.ColumnSchema{Dtype: database.DtypeFloat, Nullable: true}},
		// {"NULLIF(my_float_column, 12)", database.ColumnSchema{Dtype: database.DtypeFloat, Nullable: true}}, // once we implement case folding...

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
