package expr

import (
	"errors"
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
		{"-2", &Expression{etype: exprUnaryMinus, children: []*Expression{
			{etype: exprLiteralInt, value: "2"},
		}}},
		{"-foo", &Expression{etype: exprUnaryMinus, children: []*Expression{
			{etype: exprIdentifier, value: "foo"},
		}}},
		{"-\"Some column\"", &Expression{etype: exprUnaryMinus, children: []*Expression{
			{etype: exprIdentifierQuoted, value: "Some column"},
		}}},
		{"NOT foo", &Expression{etype: exprNot, children: []*Expression{
			{etype: exprIdentifier, value: "foo"},
		}}},
		{"NOT true", &Expression{etype: exprNot, children: []*Expression{
			{etype: exprLiteralBool, value: "TRUE"},
		}}},
		{"-(foo*bar)", &Expression{etype: exprUnaryMinus, children: []*Expression{
			{etype: exprMultiplication, parens: true, children: []*Expression{
				{etype: exprIdentifier, value: "foo"},
				{etype: exprIdentifier, value: "bar"},
			}},
		}}},
		// TODO(next): unary plus? Just eliminate the plus entirely? (tests are in place already)
		// {"+2", &Expression{etype: exprLiteralInt, value: "2"}},
		// {"+2.4", &Expression{etype: exprLiteralFloat, value: "2.4"}},

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
			{etype: exprUnaryMinus, children: []*Expression{{etype: exprLiteralInt, value: "4"}}},
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
		{"foo is true", &Expression{etype: exprEquality, children: []*Expression{
			{etype: exprIdentifier, value: "foo"},
			{etype: exprLiteralBool, value: "TRUE"},
		}}},
		{"foo is not true", &Expression{etype: exprEquality, children: []*Expression{
			{etype: exprIdentifier, value: "foo"},
			{etype: exprNot, children: []*Expression{
				{etype: exprLiteralBool, value: "TRUE"},
			}},
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
				{etype: exprUnaryMinus, children: []*Expression{
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
		{"(4 + 3) - 2*3", &Expression{etype: exprSubtraction, children: []*Expression{
			{etype: exprAddition, parens: true, children: []*Expression{
				{etype: exprLiteralInt, value: "4"},
				{etype: exprLiteralInt, value: "3"},
			}},
			{etype: exprMultiplication, children: []*Expression{
				{etype: exprLiteralInt, value: "2"},
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

		{"sum(foo < 3)", nil},
		{"sum(foo >= 3)", nil},
		{"sum(foo <= 3)", nil},
		{"count()", &Expression{etype: exprFunCall, value: "count"}},
		// TODO: make this work at some point - even in other places (e.g. `select foo, *, bar from ...`)
		//       think about ways to implement it without it being super hacky
		// {"count(*)", &Expression{etype: exprFunCall, value: "count"}},
		{"count(foobar)", &Expression{etype: exprFunCall, value: "count", children: []*Expression{
			{etype: exprIdentifier, value: "foobar"},
		}}},
		{"count(1, 2, 3)", &Expression{etype: exprFunCall, value: "count", children: []*Expression{
			{etype: exprLiteralInt, value: "1"},
			{etype: exprLiteralInt, value: "2"},
			{etype: exprLiteralInt, value: "3"},
		}}},
		{"count(1, 2*3, 3)", &Expression{etype: exprFunCall, value: "count", children: []*Expression{
			{etype: exprLiteralInt, value: "1"},
			{etype: exprMultiplication, children: []*Expression{
				{etype: exprLiteralInt, value: "2"},
				{etype: exprLiteralInt, value: "3"},
			}},
			{etype: exprLiteralInt, value: "3"},
		}}},
		// case insensitivity of function names
		{"COUNT(foobar)", &Expression{etype: exprFunCall, value: "count", children: []*Expression{
			{etype: exprIdentifier, value: "foobar"},
		}}},
		{"Count(foobar)", &Expression{etype: exprFunCall, value: "count", children: []*Expression{
			{etype: exprIdentifier, value: "foobar"},
		}}},
		{"counT(foobar)", &Expression{etype: exprFunCall, value: "count", children: []*Expression{
			{etype: exprIdentifier, value: "foobar"},
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
			{etype: exprLiteralString, value: "foo"},
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
				{etype: exprLiteralBool, value: "TRUE"},
			}},
		}}},
		{"sum(foo > 3)", &Expression{etype: exprFunCall, value: "sum", children: []*Expression{
			{etype: exprGreaterThan, children: []*Expression{
				{etype: exprIdentifier, value: "foo"},
				{etype: exprLiteralInt, value: "3"},
			}},
		}}},
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

func TestParsingErrors(t *testing.T) {
	tests := []struct {
		raw string
		err error
	}{
		{"", errUnsupportedPrefixToken},  // TODO(next): is this right?
		{" ", errUnsupportedPrefixToken}, // dtto: is this right?
		{"-", errUnsupportedPrefixToken}, // dtto: is this right?
		{"123123123131231231312312313123", errInvalidInteger},
		{"1e12312312323", errInvalidFloat},
		{"2 * (3-foo", errNoClosingBracket},
		{"foo + sum(bar", errNoClosingBracket},
		{"foo + sum(bar, ", errUnsupportedPrefixToken}, // ARCH: this is errNoClosingBracket, but we got to EOF first
		{"+123", errUnsupportedPrefixToken},
		{"foo as bar", errUnparsedBit},
	}

	for _, test := range tests {
		if _, err := ParseStringExpr(test.raw); !errors.Is(err, test.err) {
			t.Errorf("expecting %v case to fail with %v, it returned %v", test.raw, test.err, err)
		}
	}
}

func TestListParsingContents(t *testing.T) {
	tests := []struct {
		list       string
		individual []string
	}{
		{"foo", []string{"foo"}},
		{"123", []string{"123"}},
		{"1+23", []string{"1+23"}},
		{"foo, bar", []string{"foo", "bar"}},
		{"foo, bar,baz,bak", []string{"foo", "bar", "baz", "bak"}},
		{"1+2, 3+4, foo + 3, 5*(1-foo)", []string{"1+2", "3+4", "foo+3", "5*(1-foo)"}},
	}

testloop:
	for _, test := range tests {
		parsed, err := ParseStringExprs(test.list)
		if err != nil {
			t.Errorf("expression list %+v failed: %v", test.list, err)
			continue
		}

		var ip ExpressionList
		for _, expr := range test.individual {
			iparsed, err := ParseStringExpr(expr)
			if err != nil {
				t.Error(err)
				continue testloop
			}
			ip = append(ip, iparsed)
		}

		if !reflect.DeepEqual(parsed, ip) {
			t.Errorf("expecting %s to parse the same way as %s, got %v instead", test.list, test.individual, parsed)
		}
	}
}

func TestParsingSQL(t *testing.T) {
	tests := []struct {
		raw string // TODO(PR): roundtrips?
		// individual []string
		err error
	}{
		// {"WITH foo", errSQLOnlySelects},
		// {"SELECT 1", nil}, // TODO(next): support dataset-less selects
		// TODO(PR/next): we're currently using `select foo from v02fdb3...`, but we'll likely use something
		// like `select froo from bar@v02fdb3...`
		{"SELECT foo FROM bar", nil},
		// {"SELECT foo FROM bar@latest", nil}, // ARCH: do we allow 'latest' to be explicit?
		{"SELECT foo FROM bar@v020485a2686b8d38fe WHERE foo > 2", nil},
		{"SELECT foo FROM bar WHERE 1=1 and foo > bar", nil},
		{"SELECT foo FROM bar WHERE 1=1 and foo > bar GROUP BY foo", nil},
		{"SELECT foo FROM bar GROUP BY foo", nil},
		{"SELECT foo FROM bar GROUP BY foo LIMIT 2", nil},
		{"SELECT foo FROM bar@v020485a2686b8d38fe LIMIT 200", nil},
		// TODO(PR): error reporting:
		// {"SELECT foo FROM v020485a2686b8d38fe GROUP BY foo LIMIT foo", nil},
		// {"SELECT foo FROM v020485a2686b8d38fe GROUP on foo", nil},
	}

	for _, test := range tests {
		parsed, err := ParseQuerySQL(test.raw)
		if !errors.Is(err, test.err) {
			t.Errorf("when parsing SQL query %v encountered %v, expected %v", test.raw, err, test.err)
			continue
		}
		_ = parsed // TODO(PR)
	}
}
