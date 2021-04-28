package expr

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/kokes/smda/src/column"
)

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
			t.Errorf("expecting %+v to dedupe into %+v, got %+v instead", test.input, test.output, deduped)
		}
	}
}

func dummySchema(cols ...string) (schema column.TableSchema) {
	for _, col := range cols {
		schema = append(schema, column.Schema{Name: col})
	}
	return
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

	schema := dummySchema("foo", "bar", "bak", "a", "b", "c", "d")
	for _, test := range tests {
		expr, err := ParseStringExpr(test.rawExpr)
		if err != nil {
			t.Errorf("cannot parse %+v, got %+v", test.rawExpr, err)
			continue
		}
		used := expr.ColumnsUsed(schema)
		if !reflect.DeepEqual(used, test.colsUsed) {
			t.Errorf("expecting %+v to use %+v, but got %+v instead", test.rawExpr, test.colsUsed, used)
		}
	}
}

func TestCoalesceColumns(t *testing.T) {
	tests := []struct {
		types         []column.Dtype
		expectedType  column.Dtype
		expectedError error
	}{
		{nil, column.DtypeInvalid, errNoTypes},
		// single column -> same type
		{[]column.Dtype{column.DtypeBool}, column.DtypeBool, nil},
		{[]column.Dtype{column.DtypeInt}, column.DtypeInt, nil},
		{[]column.Dtype{column.DtypeFloat}, column.DtypeFloat, nil},
		// multiple of same type -> same type
		{[]column.Dtype{column.DtypeBool, column.DtypeBool, column.DtypeBool}, column.DtypeBool, nil},
		{[]column.Dtype{column.DtypeDatetime, column.DtypeDatetime, column.DtypeDatetime}, column.DtypeDatetime, nil},
		// int/float mismatch -> float
		{[]column.Dtype{column.DtypeInt, column.DtypeFloat}, column.DtypeFloat, nil},
		{[]column.Dtype{column.DtypeFloat, column.DtypeInt}, column.DtypeFloat, nil},
		// everything else -> err
		{[]column.Dtype{column.DtypeFloat, column.DtypeString}, column.DtypeInvalid, errTypeMismatch},
		{[]column.Dtype{column.DtypeBool, column.DtypeDatetime}, column.DtypeInvalid, errTypeMismatch},
		{[]column.Dtype{column.DtypeBool, column.DtypeBool, column.DtypeBool, column.DtypeInt}, column.DtypeInvalid, errTypeMismatch},
	}

	for _, test := range tests {
		dtype, err := coalesceType(test.types...)
		if dtype != test.expectedType {
			t.Errorf("expecting coalesce(%+v) to result in %+v, got %+v instead", test.types, test.expectedType, dtype)
		}
		if err != test.expectedError {
			t.Errorf("expecting coalesce(%+v) to result in err %+v, got %+v instead", test.types, test.expectedError, err)
		}
	}
}

func TestColumnsUsedVarargs(t *testing.T) {
	tests := []struct {
		rawExprs []string
		colsUsed []string
	}{
		{[]string{"1=1", "3>1"}, nil},
		{[]string{"1=1", "3>foo"}, []string{"foo"}},
		{[]string{"zoo > 3", "3>foo"}, []string{"foo", "zoo"}},
		{[]string{"2*foo > bar-bak"}, []string{"bak", "bar", "foo"}},
		{[]string{"(2*foo > bar-bak) = true"}, []string{"bak", "bar", "foo"}},
		{[]string{"1=bak", "3>foo", "bar"}, []string{"bak", "bar", "foo"}},
		{[]string{"a > 2*a -b", "3=a", "b < a*4"}, []string{"a", "b"}}, // dupes
		{[]string{"coalesce(a, c, b)"}, []string{"a", "b", "c"}},       // we return columns sorted
	}

	schema := dummySchema("foo", "zoo", "bar", "bak", "a", "b", "c")
	for _, test := range tests {
		var exprs []*Expression
		for _, rawExpr := range test.rawExprs {
			expr, err := ParseStringExpr(rawExpr)
			if err != nil {
				t.Errorf("cannot parse %+v, got %+v", rawExpr, err)
				continue
			}
			exprs = append(exprs, expr)
		}
		used := ColumnsUsed(schema, exprs...)
		if !reflect.DeepEqual(used, test.colsUsed) {
			t.Errorf("expecting %+v to use %+v, but got %+v instead", test.rawExprs, test.colsUsed, used)
		}
	}
}

func TestValidity(t *testing.T) {
	schema := column.TableSchema([]column.Schema{
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
			t.Errorf("cannot parse %+v, got %+v", raw, err)
			continue
		}
		if _, err := expr.ReturnType(schema); err != nil {
			t.Errorf("expecting %+v to be a valid expression, got: %+v", raw, err)
		}
	}
}

func TestValiditySadPaths(t *testing.T) {
	schema := column.TableSchema([]column.Schema{
		{Name: "my_int_column", Dtype: column.DtypeInt},
		{Name: "my_float_column", Dtype: column.DtypeFloat},
		{Name: "my_bool_column", Dtype: column.DtypeBool},
	})
	exprs := []string{
		"1 = 'bus'", "1 > 'foo'",
		"'bar' = my_int_column",
		"my_int_column > 3 AND my_float_column",
		"my_bool_column + my_float_column",
		// non-existing functions
		"foobar(my_int_column)",
	}

	for _, raw := range exprs {
		expr, err := ParseStringExpr(raw)
		if err != nil {
			t.Errorf("cannot parse %+v, got %+v", raw, err)
			continue
		}
		if _, err := expr.ReturnType(schema); err == nil {
			t.Errorf("expecting %+v to be an invalid expression", raw)
		}
	}
}

func TestReturnTypes(t *testing.T) {
	schema := column.TableSchema([]column.Schema{
		{Name: "my_bool_column", Dtype: column.DtypeBool},
		{Name: "my_int_column", Dtype: column.DtypeInt},
		{Name: "my_float_column", Dtype: column.DtypeFloat},
		{Name: "my_Float_column", Dtype: column.DtypeInt}, // this is intentionally incorrect
		{Name: "my_string_column", Dtype: column.DtypeString},
	})
	testCases := []struct {
		rawExpr    string
		returnType column.Schema
		err        error
	}{
		// case sensitivity
		{"my_float_column - 3", column.Schema{Dtype: column.DtypeFloat}, nil},
		{"2*my_Float_column - 3", column.Schema{Dtype: column.DtypeFloat}, nil},   // not quoted
		{"2*\"my_Float_column\" - 3", column.Schema{Dtype: column.DtypeInt}, nil}, // quoted
		// literals
		{"1", column.Schema{Dtype: column.DtypeInt}, nil},
		{"1.23", column.Schema{Dtype: column.DtypeFloat}, nil},
		{"true", column.Schema{Dtype: column.DtypeBool}, nil},
		{"'ahoy'", column.Schema{Dtype: column.DtypeString}, nil},
		{"my_int_column", column.Schema{Dtype: column.DtypeInt}, nil},

		// unary/prefix
		{"-my_int_column", column.Schema{Dtype: column.DtypeInt}, nil},
		{"-my_float_column", column.Schema{Dtype: column.DtypeFloat}, nil},
		{"-my_string_column", column.Schema{}, errTypeMismatch},
		{"not my_bool_column", column.Schema{Dtype: column.DtypeBool}, nil},
		{"not (my_int_column > 3)", column.Schema{Dtype: column.DtypeBool}, nil},
		{"not my_string_column", column.Schema{}, errTypeMismatch},
		{"not my_int_column", column.Schema{}, errTypeMismatch},

		// arithmetics
		{"1 = 1", column.Schema{Dtype: column.DtypeBool}, nil},
		{"1 != 1", column.Schema{Dtype: column.DtypeBool}, nil},
		{"1.2 > 1.3", column.Schema{Dtype: column.DtypeBool}, nil},
		{"1.2 >= 1.3", column.Schema{Dtype: column.DtypeBool}, nil},
		{"1.2 < 1.3", column.Schema{Dtype: column.DtypeBool}, nil},
		{"1.2 <= 1.3", column.Schema{Dtype: column.DtypeBool}, nil},
		{"my_float_column = 123", column.Schema{Dtype: column.DtypeBool}, nil},
		{"my_float_column = my_int_column", column.Schema{Dtype: column.DtypeBool}, nil},
		{"my_float_column <= my_int_column", column.Schema{Dtype: column.DtypeBool}, nil},
		{"my_float_column > 12.234", column.Schema{Dtype: column.DtypeBool}, nil},
		{"my_float_column > my_int_column", column.Schema{Dtype: column.DtypeBool}, nil},

		// arithmetics with nulls
		{"my_float_column = null", column.Schema{Dtype: column.DtypeBool}, nil},
		{"my_float_column != null", column.Schema{Dtype: column.DtypeBool}, nil},
		{"my_float_column > null", column.Schema{Dtype: column.DtypeBool}, nil},
		{"null = my_float_column", column.Schema{Dtype: column.DtypeBool}, nil},
		{"null != my_float_column", column.Schema{Dtype: column.DtypeBool}, nil},
		{"null < my_float_column", column.Schema{Dtype: column.DtypeBool}, nil},

		{"1 + 2", column.Schema{Dtype: column.DtypeInt}, nil},
		{"1 - 2", column.Schema{Dtype: column.DtypeInt}, nil},
		{"1 * 2", column.Schema{Dtype: column.DtypeInt}, nil},
		{"1 / 2", column.Schema{Dtype: column.DtypeFloat}, nil},
		{"4 - my_int_column", column.Schema{Dtype: column.DtypeInt}, nil},
		{"4 / my_int_column", column.Schema{Dtype: column.DtypeFloat}, nil},
		{"my_float_column / my_int_column", column.Schema{Dtype: column.DtypeFloat}, nil},
		{"1 + null", column.Schema{Dtype: column.DtypeInt}, nil},
		{"null + 1", column.Schema{Dtype: column.DtypeInt}, nil},

		// and/or
		{"my_float_column > 3 AND my_int_column = 4", column.Schema{Dtype: column.DtypeBool}, nil},
		{"my_float_column > 3 OR my_int_column = 4", column.Schema{Dtype: column.DtypeBool}, nil},

		// function calls
		{"count()", column.Schema{Dtype: column.DtypeInt}, nil},
		{"count(my_int_column)", column.Schema{Dtype: column.DtypeInt}, nil},
		{"nullif(my_int_column, 12)", column.Schema{Dtype: column.DtypeInt, Nullable: true}, nil},
		{"nullif(my_float_column, 12)", column.Schema{Dtype: column.DtypeFloat, Nullable: true}, nil},
		{"14*min(my_float_column)", column.Schema{Dtype: column.DtypeFloat, Nullable: false}, nil},
		{"14*max(my_float_column)", column.Schema{Dtype: column.DtypeFloat, Nullable: false}, nil},
		{"14*min(my_int_column)", column.Schema{Dtype: column.DtypeInt, Nullable: false}, nil},
		{"14*max(my_int_column)", column.Schema{Dtype: column.DtypeInt, Nullable: false}, nil},
		{"sum(my_int_column)", column.Schema{Dtype: column.DtypeInt, Nullable: false}, nil},
		{"sum(my_float_column)", column.Schema{Dtype: column.DtypeFloat, Nullable: false}, nil},
		{"avg(my_int_column)", column.Schema{Dtype: column.DtypeFloat, Nullable: false}, nil},
		{"avg(my_float_column)", column.Schema{Dtype: column.DtypeFloat, Nullable: false}, nil},
		{"round(my_int_column)", column.Schema{Dtype: column.DtypeFloat, Nullable: false}, nil},
		{"round(my_float_column)", column.Schema{Dtype: column.DtypeFloat, Nullable: false}, nil},
		{"round(my_int_column, 3)", column.Schema{Dtype: column.DtypeFloat, Nullable: false}, nil},
		{"round(my_float_column, 4)", column.Schema{Dtype: column.DtypeFloat, Nullable: false}, nil},

		// trigonometric functions always return a nullable column (though sin/cos/exp don't have to)
		{"sin(my_float_column)", column.Schema{Dtype: column.DtypeFloat, Nullable: true}, nil},
		{"cos(my_float_column)", column.Schema{Dtype: column.DtypeFloat, Nullable: true}, nil},
		{"exp(my_float_column)", column.Schema{Dtype: column.DtypeFloat, Nullable: true}, nil},
		{"sin(my_int_column)", column.Schema{Dtype: column.DtypeFloat, Nullable: true}, nil},
		{"cos(my_int_column)", column.Schema{Dtype: column.DtypeFloat, Nullable: true}, nil},
		{"exp(my_int_column)", column.Schema{Dtype: column.DtypeFloat, Nullable: true}, nil},

		// argument counts
		{"count(my_int_column, 3)", column.Schema{}, errWrongNumberofArguments},
		{"count(true, 3)", column.Schema{}, errWrongNumberofArguments},
		{"min(4, 3)", column.Schema{}, errWrongNumberofArguments},
		{"max(4, 3)", column.Schema{}, errWrongNumberofArguments},
		{"min()", column.Schema{}, errWrongNumberofArguments},
		{"max()", column.Schema{}, errWrongNumberofArguments},
		{"avg()", column.Schema{}, errWrongNumberofArguments},
		{"sum()", column.Schema{}, errWrongNumberofArguments},
		{"sum(my_int_column, my_float_column)", column.Schema{}, errWrongNumberofArguments},
		{"round()", column.Schema{}, errWrongNumberofArguments},
		{"round(my_float_column, 3, 4)", column.Schema{}, errWrongNumberofArguments},
		{"sin()", column.Schema{}, errWrongNumberofArguments},
		{"cos()", column.Schema{}, errWrongNumberofArguments},
		{"exp()", column.Schema{}, errWrongNumberofArguments},
		{"log()", column.Schema{}, errWrongNumberofArguments},
		{"tanh()", column.Schema{}, errWrongNumberofArguments},
		{"sin(1,2)", column.Schema{}, errWrongNumberofArguments},
		{"cos(3,4)", column.Schema{}, errWrongNumberofArguments},
		{"exp(5,6)", column.Schema{}, errWrongNumberofArguments},
		{"log(7,8)", column.Schema{}, errWrongNumberofArguments},
		{"tanh(9,10)", column.Schema{}, errWrongNumberofArguments},
		{"nullif()", column.Schema{}, errWrongNumberofArguments},
		{"nullif(my_int_column)", column.Schema{}, errWrongNumberofArguments},
		{"nullif(my_int_column, 4, 5)", column.Schema{}, errWrongNumberofArguments},
		{"coalesce()", column.Schema{}, errWrongNumberofArguments},

		{"sum(my_string_column)", column.Schema{}, errWrongArgumentType},
		// {"NULLIF(my_float_column, 12)", column.Schema{Dtype: column.DtypeFloat, Nullable: true}, nil}, // once we implement case folding...

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
		if !errors.Is(err, test.err) {
			t.Errorf("expecting ReturnType(%+v) to result in err %+v, got %+v instead", test.rawExpr, test.err, err)
			continue
		}
		retType.Name = "" // resetting the name, we're not comparing it here
		if !reflect.DeepEqual(retType, test.returnType) {
			t.Errorf("expecting %+v to return a schema %+v, got %+v instead", test.rawExpr, test.returnType, retType)
		}

	}
}
