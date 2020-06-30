package expr

import (
	"reflect"
	"testing"

	"github.com/kokes/smda/src/database"
)

// TODO: test IsValid

func TestReturnTypes(t *testing.T) {
	schema := database.TableSchema{}
	testCases := []struct {
		rawExpr    string
		returnType database.ColumnSchema
	}{
		{"1", database.ColumnSchema{Dtype: database.DtypeInt}},
		{"1 = 1", database.ColumnSchema{Dtype: database.DtypeBool}},
		{"1 != 1", database.ColumnSchema{Dtype: database.DtypeBool}},
		{"1.2 > 1.3", database.ColumnSchema{Dtype: database.DtypeBool}},
		{"1.2 >= 1.3", database.ColumnSchema{Dtype: database.DtypeBool}},
		{"1.2 < 1.3", database.ColumnSchema{Dtype: database.DtypeBool}},
		{"1.2 <= 1.3", database.ColumnSchema{Dtype: database.DtypeBool}},

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
