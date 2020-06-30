package expr

import (
	"reflect"
	"testing"

	"github.com/kokes/smda/src/database"
)

// TODO: test IsValid

func TestReturnTypes(t *testing.T) {
	schema := database.TableSchema([]database.ColumnSchema{
		{Name: "my_int_column", Dtype: database.DtypeInt},
		{Name: "my_float_column", Dtype: database.DtypeFloat},
	})
	testCases := []struct {
		rawExpr    string
		returnType database.ColumnSchema
	}{
		{"1", database.ColumnSchema{Dtype: database.DtypeInt}},
		{"1.23", database.ColumnSchema{Dtype: database.DtypeFloat}},
		// {"true", database.ColumnSchema{Dtype: database.DtypeBool}},
		{"'ahoy'", database.ColumnSchema{Dtype: database.DtypeString}},
		{"my_int_column", database.ColumnSchema{Dtype: database.DtypeInt, Name: "my_int_column"}},

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
