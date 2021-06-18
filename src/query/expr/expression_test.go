package expr

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/kokes/smda/src/column"
)

func TestAggExpr(t *testing.T) {
	tests := []struct {
		raw      string
		expected []string
		err      error
	}{
		{"1", nil, nil},
		{"nullif(foo)", nil, nil},
		{"2*nullif(foo)", nil, nil},
		{"nullif(foo)*2", nil, nil},
		{"1 + nullif(foo) - bar", nil, nil},
		{"min(a)", []string{"min(a)"}, nil},
		{"min(a) + min(b)", []string{"min(a)", "min(b)"}, nil},
		{"4*min(a) + 3-min(b)", []string{"min(a)", "min(b)"}, nil},
		{"2*nullif(min(a) + 3*min(b))", []string{"min(a)", "min(b)"}, nil},
		{"min(a)*min(b)*min(c)", []string{"min(a)", "min(b)", "min(c)"}, nil},
		// nested aggexprs
		{"min(5*min(a))", nil, errNoNestedAggregations},
		{"sum(max(b))", nil, errNoNestedAggregations},
		{"1-sum(nullif(foo, max(bar)))", nil, errNoNestedAggregations},
	}
	for _, test := range tests {
		expr, err := ParseStringExpr(test.raw)
		if err != nil {
			t.Error(err)
			continue
		}
		res, err := AggExpr(expr)
		if err != test.err {
			t.Errorf("expecting %s to result in error %+v, got %+v instead", test.raw, test.err, err)
		}
		var ress []string
		for _, el := range res {
			ress = append(ress, el.String())
		}
		if !reflect.DeepEqual(ress, test.expected) {
			t.Errorf("expected %+v to have %+v as aggregating expressions, got %+v instead", test.raw, test.expected, ress)
		}
	}
}

func TestExprStringer(t *testing.T) {
	tests := []struct {
		raw      string
		expected string
	}{
		{"1+2+ 3", "1+2+3"},
		{"1+(2+ 3)", "1+(2+3)"},
		{"max( foo) - 3", "max(foo)-3"},
		{"2 * (foo-BAR)", "2*(foo-bar)"},
		{"(foo-BAR)*2", "(foo-bar)*2"},
		{"(foo-(3-BAR))*2", "(foo-(3-bar))*2"},
		{"foo = 'bar'", "foo='bar'"},
		{"not true", "NOT TRUE"},
		{"not  (1+2+ 3)", "NOT (1+2+3)"},
		{"foo as bar", "foo AS bar"},
		{"1+2*3 as bar", "1+2*3 AS bar"},
		// these are the only three infix operators that have spaces around the op
		{"foo is bar", "foo IS bar"},
		{"foo and bar", "foo AND bar"},
		{"foo or bar", "foo OR bar"},
		// ... and these do not (not exhaustive)
		{"foo=bar", "foo=bar"},
		{"foo > bar", "foo>bar"},
		{"foo <= bar", "foo<=bar"},
	}

	for _, test := range tests {
		parsed, err := ParseStringExpr(test.raw)
		if err != nil {
			t.Fatalf("expression %+v failed: %v", test.raw, err)
			continue
		}
		if parsed.String() != test.expected {
			t.Errorf("expecting %s to parse and then stringify into %s, got %s instead", test.raw, test.expected, parsed.String())
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
		{"foo as bar", []string{"foo"}},
		{"foo*bar as bak", []string{"bar", "foo"}},
		{"1 as foo", nil},
	}

	schema := dummySchema("foo", "bar", "bak", "a", "b", "c", "d")
	for _, test := range tests {
		expr, err := ParseStringExpr(test.rawExpr)
		if err != nil {
			t.Errorf("cannot parse %+v, got %+v", test.rawExpr, err)
			continue
		}
		used := ColumnsUsed(expr, schema)
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
		var exprs []Expression
		for _, rawExpr := range test.rawExprs {
			expr, err := ParseStringExpr(rawExpr)
			if err != nil {
				t.Errorf("cannot parse %+v, got %+v", rawExpr, err)
				continue
			}
			exprs = append(exprs, expr)
		}
		used := ColumnsUsedMultiple(schema, exprs...)
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
		{"+my_int_column", column.Schema{Dtype: column.DtypeInt}, nil},
		{"+my_float_column", column.Schema{Dtype: column.DtypeFloat}, nil},
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

		// string functions
		{"trim(my_string_column)", column.Schema{Dtype: column.DtypeString, Nullable: false}, nil},
		{"upper(my_string_column)", column.Schema{Dtype: column.DtypeString, Nullable: false}, nil},
		{"lower(my_string_column)", column.Schema{Dtype: column.DtypeString, Nullable: false}, nil},
		{"left(my_string_column, 4)", column.Schema{Dtype: column.DtypeString, Nullable: false}, nil},
		// {"mid(my_string_column, 4)", column.Schema{Dtype: column.DtypeString, Nullable: false}, nil},
		// {"right(my_string_column, 4)", column.Schema{Dtype: column.DtypeString, Nullable: false}, nil},
		{"split_part(my_string_column, 'foo', 4)", column.Schema{Dtype: column.DtypeString, Nullable: false}, nil},

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
		{"left(my_string_column)", column.Schema{}, errWrongNumberofArguments},
		// {"mid(my_string_column)", column.Schema{}, errWrongNumberofArguments},
		// {"right(my_string_column)", column.Schema{}, errWrongNumberofArguments},

		{"sum(my_string_column)", column.Schema{}, errWrongArgumentType},
		// {"NULLIF(my_float_column, 12)", column.Schema{Dtype: column.DtypeFloat, Nullable: true}, nil}, // once we implement case folding...

		// "ahoy", "foo / bar", "2 * foo", "2+3*4", "count(foobar)", "bak = 'my literal'",
		// "coalesce(foo, bar, 1) - 4", "nullif(baz, 'foo')", "nullif(bak, 103)",
		// "round(1.234, 2)", "count(foo = true)", "bak != 3",
		// "sum(foo > 3)", "sum(foo < 3)", "sum(foo >= 3)", "sum(foo <= 3)",

		// relabeling
		{"my_string_column as foo", column.Schema{Name: "foo", Dtype: column.DtypeString}, nil},
		{"my_string_column as \"Bar\"", column.Schema{Name: "Bar", Dtype: column.DtypeString}, nil},
		// ARCH: we no longer support this pattern
		// {"(my_string_column as foo) as bar", column.Schema{Name: "bar", Dtype: column.DtypeString}, nil},
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
		if test.returnType.Name == "" {
			retType.Name = "" // resetting the name, we're not comparing it here
		}
		if !reflect.DeepEqual(retType, test.returnType) {
			t.Errorf("expecting %+v to return a schema %+v, got %+v instead", test.rawExpr, test.returnType, retType)
		}

	}
}

func TestHasIdentifiers(t *testing.T) {
	tests := []struct {
		raw string
		has bool
	}{
		{"1", false},
		{"1+2", false},
		{"foo", true},
		{"1 + foo", true},
		{"sum(foo)", true},
		{"sum(foo) + 4", true},
		{"2/3 + round(bar)", true},
		{"2/3 + round(\"Baz\")", true},
	}

	for _, test := range tests {
		expr, err := ParseStringExpr(test.raw)
		if err != nil {
			t.Errorf("cannot parse %+v, got %+v", test.raw, err)
			continue
		}
		has := HasIdentifiers(expr)
		if has != test.has {
			t.Errorf("expecting HasIdentifiers(%v) to result in %v, got %v instead", test.raw, test.has, has)
		}
	}
}
