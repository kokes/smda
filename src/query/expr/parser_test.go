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
		// standalone expressions and literals
		{"ahoy", &Expression{etype: exprIdentifier, value: "ahoy"}},
		{"type", &Expression{etype: exprIdentifier, value: "type"}},
		{"for", &Expression{etype: exprIdentifier, value: "for"}},
		{"struct", &Expression{etype: exprIdentifier, value: "struct"}},
		{"break", &Expression{etype: exprIdentifier, value: "break"}},
		{"func", &Expression{etype: exprIdentifier, value: "func"}},
		{"\"ahoy\"", &Expression{etype: exprIdentifier, value: "ahoy"}},
		{"\"ahoy_world\"", &Expression{etype: exprIdentifier, value: "ahoy_world"}},
		{"\"ahoy62\"", &Expression{etype: exprIdentifier, value: "ahoy62"}},
		{"\"hello world\"", &Expression{etype: exprIdentifierQuoted, value: "hello world"}},
		{"254", &Expression{etype: exprLiteralInt, value: "254"}},
		{"254.678", &Expression{etype: exprLiteralFloat, value: "254.678"}},
		{"true", &Expression{etype: exprLiteralBool, value: "TRUE"}},
		{"TRUE", &Expression{etype: exprLiteralBool, value: "TRUE"}},
		{"True", &Expression{etype: exprLiteralBool, value: "TRUE"}},
		{"false", &Expression{etype: exprLiteralBool, value: "FALSE"}},
		{"FALSE", &Expression{etype: exprLiteralBool, value: "FALSE"}},
		{"'foo'", &Expression{etype: exprLiteralString, value: "foo"}},
		{"'foo bar'", &Expression{etype: exprLiteralString, value: "foo bar"}},
		{"'foo'' bar'", &Expression{etype: exprLiteralString, value: "foo' bar"}},
		{"null", &Expression{etype: exprLiteralNull}},
		{"NULL", &Expression{etype: exprLiteralNull}},
		{"NULl", &Expression{etype: exprLiteralNull}},

		// prefix operators
		// TODO(PR): test just "-" - to see if advancing tokens will fail our parser
		{"-2", &Expression{etype: exprPrefixOperator, value: "-", children: []*Expression{
			{etype: exprLiteralInt, value: "2"},
		}}},
		{"-foo", &Expression{etype: exprPrefixOperator, value: "-", children: []*Expression{
			{etype: exprIdentifier, value: "foo"},
		}}},
		{"-\"Some column\"", &Expression{etype: exprPrefixOperator, value: "-", children: []*Expression{
			{etype: exprIdentifierQuoted, value: "Some column"},
		}}},
		{"NOT foo", &Expression{etype: exprPrefixOperator, value: "NOT", children: []*Expression{
			{etype: exprIdentifier, value: "foo"},
		}}},
		{"NOT true", &Expression{etype: exprPrefixOperator, value: "NOT", children: []*Expression{
			{etype: exprLiteralBool, value: "TRUE"},
		}}},
		{"-(foo*bar)", &Expression{etype: exprPrefixOperator, value: "-", children: []*Expression{
			{etype: exprMultiplication, parens: true, children: []*Expression{
				{etype: exprIdentifier, value: "foo"},
				{etype: exprIdentifier, value: "bar"},
			}},
		}}},
		// TODO(PR): unary plus? Just eliminate the plus entirely?

		// infix operators
		{"4 * 2", &Expression{etype: exprMultiplication, children: []*Expression{
			{etype: exprLiteralInt, value: "4"},
			{etype: exprLiteralInt, value: "2"},
		}}},
		{"4 + foo", &Expression{etype: exprAddition, children: []*Expression{
			{etype: exprLiteralInt, value: "4"},
			{etype: exprIdentifier, value: "foo"},
		}}},
		{"4 - foo", &Expression{etype: exprSubtraction, children: []*Expression{
			{etype: exprLiteralInt, value: "4"},
			{etype: exprIdentifier, value: "foo"},
		}}},
		{"4 / foo", &Expression{etype: exprDivision, children: []*Expression{
			{etype: exprLiteralInt, value: "4"},
			{etype: exprIdentifier, value: "foo"},
		}}},
		{"4 + 3 + 2", &Expression{etype: exprAddition, children: []*Expression{
			{etype: exprAddition, children: []*Expression{
				{etype: exprLiteralInt, value: "4"},
				{etype: exprLiteralInt, value: "3"},
			}},
			{etype: exprLiteralInt, value: "2"},
		}}},
		{"4 + 3 * 2", &Expression{etype: exprAddition, children: []*Expression{
			{etype: exprLiteralInt, value: "4"},
			{etype: exprMultiplication, children: []*Expression{
				{etype: exprLiteralInt, value: "3"},
				{etype: exprLiteralInt, value: "2"},
			}},
		}}},
		{"2 * \"ahoy\"", &Expression{etype: exprMultiplication, children: []*Expression{
			{etype: exprLiteralInt, value: "2"},
			{etype: exprIdentifier, value: "ahoy"},
		}}},
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

		// prefix and infix
		{"-4 / foo", &Expression{etype: exprDivision, children: []*Expression{
			{etype: exprPrefixOperator, value: "-", children: []*Expression{{etype: exprLiteralInt, value: "4"}}},
			{etype: exprIdentifier, value: "foo"},
		}}},

		// operators
		{"4 + 3 > 5", &Expression{etype: exprGreaterThan, children: []*Expression{
			{etype: exprAddition, children: []*Expression{
				{etype: exprLiteralInt, value: "4"},
				{etype: exprLiteralInt, value: "3"},
			}},
			{etype: exprLiteralInt, value: "5"},
		}}},
		{"4 + 3 >= 5", &Expression{etype: exprGreaterThanEqual, children: []*Expression{
			{etype: exprAddition, children: []*Expression{
				{etype: exprLiteralInt, value: "4"},
				{etype: exprLiteralInt, value: "3"},
			}},
			{etype: exprLiteralInt, value: "5"},
		}}},
		{"4 > 3 = true", &Expression{etype: exprEquality, children: []*Expression{
			{etype: exprGreaterThan, children: []*Expression{
				{etype: exprLiteralInt, value: "4"},
				{etype: exprLiteralInt, value: "3"},
			}},
			{etype: exprLiteralBool, value: "TRUE"},
		}}},
		{"foo = 'bar'", &Expression{etype: exprEquality, children: []*Expression{
			{etype: exprIdentifier, value: "foo"},
			{etype: exprLiteralString, value: "bar"},
		}}},
		{"'bar' = foo", &Expression{etype: exprEquality, children: []*Expression{
			{etype: exprLiteralString, value: "bar"},
			{etype: exprIdentifier, value: "foo"},
		}}},
		{"3 != bak", &Expression{etype: exprNequality, children: []*Expression{
			{etype: exprLiteralInt, value: "3"},
			{etype: exprIdentifier, value: "bak"},
		}}},
		{"bak = 'my_literal'", &Expression{etype: exprEquality, children: []*Expression{
			{etype: exprIdentifier, value: "bak"},
			{etype: exprLiteralString, value: "my_literal"},
		}}},
		{"bak = 'my_li''ter''al'", &Expression{etype: exprEquality, children: []*Expression{
			{etype: exprIdentifier, value: "bak"},
			{etype: exprLiteralString, value: "my_li'ter'al"},
		}}},
		{"foo = true", &Expression{etype: exprEquality, children: []*Expression{
			{etype: exprIdentifier, value: "foo"},
			{etype: exprLiteralBool, value: "TRUE"},
		}}},

		// boolean operators
		{"foo and bar", &Expression{etype: exprAnd, children: []*Expression{
			{etype: exprIdentifier, value: "foo"},
			{etype: exprIdentifier, value: "bar"},
		}}},
		{"4 > 3 AND 5 = 1", &Expression{etype: exprAnd, children: []*Expression{
			{etype: exprGreaterThan, children: []*Expression{
				{etype: exprLiteralInt, value: "4"},
				{etype: exprLiteralInt, value: "3"},
			}},
			{etype: exprEquality, children: []*Expression{
				{etype: exprLiteralInt, value: "5"},
				{etype: exprLiteralInt, value: "1"},
			}},
		}}},
		{"foo = 2 AND 3 = bar", &Expression{etype: exprAnd, children: []*Expression{
			{etype: exprEquality, children: []*Expression{
				{etype: exprIdentifier, value: "foo"},
				{etype: exprLiteralInt, value: "2"},
			}},
			{etype: exprEquality, children: []*Expression{
				{etype: exprLiteralInt, value: "3"},
				{etype: exprIdentifier, value: "bar"},
			}},
		}}},
		{"foo > 3 OR -2 <= bar", &Expression{etype: exprOr, children: []*Expression{
			{etype: exprGreaterThan, children: []*Expression{
				{etype: exprIdentifier, value: "foo"},
				{etype: exprLiteralInt, value: "3"},
			}},
			{etype: exprLessThanEqual, children: []*Expression{
				{etype: exprPrefixOperator, value: "-", children: []*Expression{
					{etype: exprLiteralInt, value: "2"},
				}},
				{etype: exprIdentifier, value: "bar"},
			}},
		}}},

		// parentheses
		{"2 * (4 + 3)", &Expression{etype: exprMultiplication, children: []*Expression{
			{etype: exprLiteralInt, value: "2"},
			{etype: exprAddition, parens: true, children: []*Expression{
				{etype: exprLiteralInt, value: "4"},
				{etype: exprLiteralInt, value: "3"},
			}},
		}}},
		{"2 * (1 - foo)", &Expression{etype: exprMultiplication, children: []*Expression{
			{etype: exprLiteralInt, value: "2"},
			{etype: exprSubtraction, parens: true, children: []*Expression{
				{etype: exprLiteralInt, value: "1"},
				{etype: exprIdentifier, value: "foo"},
			}},
		}}},

		// just testing the parser, not checking the output
		{"foo = 'bar' AND bak = 'bar'", nil},
		{"1 < foo < 3", nil},
		{"bar < foo < bak", nil},

		// TODO(PR): function calls
		// {"sum(foo < 3)", nil},
		// {"sum(foo >= 3)", nil},
		// {"sum(foo <= 3)", nil},
		// {"count(foobar)", &Expression{etype: exprFunCall, value: "count", children: []*Expression{
		// 	{etype: exprIdentifier, value: "foobar"},
		// }}},
		// // case insensitivity of function names
		// {"COUNT(foobar)", &Expression{etype: exprFunCall, value: "count", children: []*Expression{
		// 	{etype: exprIdentifier, value: "foobar"},
		// }}},
		// {"Count(foobar)", &Expression{etype: exprFunCall, value: "count", children: []*Expression{
		// 	{etype: exprIdentifier, value: "foobar"},
		// }}},
		// {"counT(foobar)", &Expression{etype: exprFunCall, value: "count", children: []*Expression{
		// 	{etype: exprIdentifier, value: "foobar"},
		// }}},
		// {"coalesce(foo, bar, 1) - 4", &Expression{etype: exprSubtraction, children: []*Expression{
		// 	{etype: exprFunCall, value: "coalesce", children: []*Expression{
		// 		{etype: exprIdentifier, value: "foo"},
		// 		{etype: exprIdentifier, value: "bar"},
		// 		{etype: exprLiteralInt, value: "1"},
		// 	}},
		// 	{etype: exprLiteralInt, value: "4"},
		// }}},
		// {"nullif(baz, 'foo')", &Expression{etype: exprFunCall, value: "nullif", children: []*Expression{
		// 	{etype: exprIdentifier, value: "baz"},
		// 	{etype: exprLiteralString, value: "foo"},
		// }}},
		// {"nullif(bak, 103)", &Expression{etype: exprFunCall, value: "nullif", children: []*Expression{
		// 	{etype: exprIdentifier, value: "bak"},
		// 	{etype: exprLiteralInt, value: "103"},
		// }}},
		// {"round(1.234, 2)", &Expression{etype: exprFunCall, value: "round", children: []*Expression{
		// 	{etype: exprLiteralFloat, value: "1.234"},
		// 	{etype: exprLiteralInt, value: "2"},
		// }}},
		// {"count(foo = true)", &Expression{etype: exprFunCall, value: "count", children: []*Expression{
		// 	{etype: exprEquality, children: []*Expression{
		// 		{etype: exprIdentifier, value: "foo"},
		// 		{etype: exprLiteralBool, value: "true"},
		// 	}},
		// }}},
		// {"sum(foo > 3)", &Expression{etype: exprFunCall, value: "sum", children: []*Expression{
		// 	{etype: exprGreaterThan, children: []*Expression{
		// 		{etype: exprIdentifier, value: "foo"},
		// 		{etype: exprLiteralInt, value: "3"},
		// 	}},
		// }}},
	}

	for _, test := range tests {
		parsed, err := ParseStringExpr(test.raw)
		if err != nil {
			t.Errorf("expression %+v failed: %v", test.raw, err)
			continue
		}
		// we skip equality tests for nil cases (essentially placeholders, perhaps too complex)
		// we need to reset the assigned functions as these are not comparable
		// ARCH: we should really either a) recursively remove these functions, b) create a custom
		// function for comparing expressions (like we have with chunks [ChunksEqual])
		parsed.evaler, parsed.aggregatorFactory = nil, nil
		for _, ch := range parsed.children {
			ch.evaler, ch.aggregatorFactory = nil, nil
		}
		if test.expExpr != nil && !reflect.DeepEqual(parsed, test.expExpr) {
			t.Errorf("expecting %s to parse into %+v, got %+v instead", test.raw, test.expExpr, parsed)
		}
	}
}
