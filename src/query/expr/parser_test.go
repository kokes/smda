package expr

import (
	"reflect"
	"testing"
)

func TestParsingContents(t *testing.T) {
	tests := []struct {
		raw     string
		expExpr *Expression
	}{
		{"ahoy", &Expression{etype: exprIdentifier, value: "ahoy"}},
		{"foo / bar", &Expression{etype: exprDivision, children: []*Expression{
			{etype: exprIdentifier, value: "foo"},
			{etype: exprIdentifier, value: "bar"},
		}}},
		{"2 * foo", &Expression{etype: exprMultiplication, children: []*Expression{
			{etype: exprLiteralInt, value: "2"},
			{etype: exprIdentifier, value: "foo"},
		}}},
		{"2 + 3*4", &Expression{etype: exprAddition, children: []*Expression{
			{etype: exprLiteralInt, value: "2"},
			{etype: exprMultiplication, children: []*Expression{
				{etype: exprLiteralInt, value: "3"},
				{etype: exprLiteralInt, value: "4"},
			}},
		}}},
		{"count(foobar)", &Expression{etype: exprFunCall, value: "count", children: []*Expression{
			{etype: exprIdentifier, value: "foobar"},
		}}},
		{"bak = 'my_literal'", &Expression{etype: exprEquality, children: []*Expression{
			{etype: exprIdentifier, value: "bak"},
			{etype: exprLiteralString, value: "'my_literal'"}, // this will probably not last, should be just `my_literal`
		}}},
		{"coalesce(foo, bar, 1) - 4", &Expression{etype: exprSubtraction, children: []*Expression{
			{etype: exprFunCall, value: "coalesce", children: []*Expression{
				{etype: exprIdentifier, value: "foo"},
				{etype: exprIdentifier, value: "bar"},
				{etype: exprLiteralInt, value: "1"},
			}},
			{etype: exprLiteralInt, value: "4"},
		}}},
		{"nullif(baz, 'foo')", &Expression{etype: exprFunCall, value: "nullif", children: []*Expression{
			{etype: exprIdentifier, value: "baz"},
			{etype: exprLiteralString, value: "'foo'"},
		}}},
		{"nullif(bak, 103)", &Expression{etype: exprFunCall, value: "nullif", children: []*Expression{
			{etype: exprIdentifier, value: "bak"},
			{etype: exprLiteralInt, value: "103"},
		}}},
		{"round(1.234, 2)", &Expression{etype: exprFunCall, value: "round", children: []*Expression{
			{etype: exprLiteralFloat, value: "1.234"},
			{etype: exprLiteralInt, value: "2"},
		}}},
		{"count(foo = true)", &Expression{etype: exprFunCall, value: "count", children: []*Expression{
			{etype: exprEquality, children: []*Expression{
				{etype: exprIdentifier, value: "foo"},
				{etype: exprLiteralBool, value: "true"},
			}},
		}}},
		{"3 != bak", &Expression{etype: exprNequality, children: []*Expression{
			{etype: exprLiteralInt, value: "3"},
			{etype: exprIdentifier, value: "bak"},
		}}},
		{"sum(foo > 3)", &Expression{etype: exprFunCall, value: "sum", children: []*Expression{
			{etype: exprGreaterThan, children: []*Expression{
				{etype: exprIdentifier, value: "foo"},
				{etype: exprLiteralInt, value: "3"},
			}},
		}}},
		{"sum(foo < 3)", nil},
		{"sum(foo >= 3)", nil},
		{"sum(foo <= 3)", nil},
		{"2 * (1 - foo)", &Expression{etype: exprMultiplication, children: []*Expression{
			{etype: exprLiteralInt, value: "2"},
			{etype: exprSubtraction, children: []*Expression{
				{etype: exprLiteralInt, value: "1"},
				{etype: exprIdentifier, value: "foo"},
			}},
		}}},
		{"foo = true", &Expression{etype: exprEquality, children: []*Expression{
			{etype: exprIdentifier, value: "foo"},
			{etype: exprLiteralBool, value: "true"},
		}}},
		{"-2", &Expression{etype: exprLiteralInt, value: "-2"}},
		{"-2.4", &Expression{etype: exprLiteralFloat, value: "-2.4"}}, // unary expressions
		{"foo = 2 && 3 = bar", &Expression{etype: exprAnd, children: []*Expression{
			{etype: exprEquality, children: []*Expression{
				{etype: exprIdentifier, value: "foo"},
				{etype: exprLiteralInt, value: "2"},
			}},
			{etype: exprEquality, children: []*Expression{
				{etype: exprLiteralInt, value: "3"},
				{etype: exprIdentifier, value: "bar"},
			}},
		}}},
		{"foo > 3 || -2 <= bar", &Expression{etype: exprOr, children: []*Expression{
			{etype: exprGreaterThan, children: []*Expression{
				{etype: exprIdentifier, value: "foo"},
				{etype: exprLiteralInt, value: "3"},
			}},
			{etype: exprLessThanEqual, children: []*Expression{
				{etype: exprLiteralInt, value: "-2"},
				{etype: exprIdentifier, value: "bar"},
			}},
		}}},
	}

	for _, test := range tests {
		parsed, err := ParseStringExpr(test.raw)
		if err != nil {
			t.Error(err)
			continue
		}
		// we skip equality tests for nil cases (essentially placeholders, perhaps too complex)
		if test.expExpr != nil && !reflect.DeepEqual(parsed, test.expExpr) {
			t.Errorf("expecting %s to parse into %s, got %s instead", test.raw, test.expExpr, parsed)
		}
	}
}

// func (expr *Expression) UnmarshalJSON(data []byte) error {
// expr.stringer
// MarshalJSON
