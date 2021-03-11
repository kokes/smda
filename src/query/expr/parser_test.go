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
		{"\"ahoy\"", &Expression{etype: exprIdentifierQuoted, value: "ahoy"}},
		{"\"hello world\"", &Expression{etype: exprIdentifierQuoted, value: "hello world"}},
		{"\"hello world\"*2", &Expression{etype: exprMultiplication, children: []*Expression{
			{etype: exprIdentifierQuoted, value: "hello world"},
			{etype: exprLiteralInt, value: "2"},
		}}},
		{"foo = 'bar' AND bak = 'bar'", nil},
		{"1 < foo < 3", nil},
		{"bar < foo < bak", nil},
		{"2 * \"ahoy\"", &Expression{etype: exprMultiplication, children: []*Expression{
			{etype: exprLiteralInt, value: "2"},
			{etype: exprIdentifierQuoted, value: "ahoy"},
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
		{"count(foobar)", &Expression{etype: exprFunCall, value: "count", children: []*Expression{
			{etype: exprIdentifier, value: "foobar"},
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
		{"bak = 'my_literal'", &Expression{etype: exprEquality, children: []*Expression{
			{etype: exprIdentifier, value: "bak"},
			{etype: exprLiteralString, value: "my_literal"},
		}}},
		{"bak = 'my_li''ter''al'", &Expression{etype: exprEquality, children: []*Expression{
			{etype: exprIdentifier, value: "bak"},
			{etype: exprLiteralString, value: "my_li'ter'al"},
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
				{etype: exprLiteralInt, value: "-2"},
				{etype: exprIdentifier, value: "bar"},
			}},
		}}},
		{"+foo", &Expression{etype: exprIdentifier, value: "foo"}},
		{"-foo", &Expression{etype: exprMultiplication, children: []*Expression{
			{etype: exprLiteralInt, value: "-1"},
			{etype: exprIdentifier, value: "foo"},
		}}},
		{"-(foo*bar)", &Expression{etype: exprMultiplication, children: []*Expression{
			{etype: exprLiteralInt, value: "-1"},
			{etype: exprMultiplication, children: []*Expression{
				{etype: exprIdentifier, value: "foo"},
				{etype: exprIdentifier, value: "bar"},
			}},
		}}},
	}

	for _, test := range tests {
		parsed, err := ParseStringExpr(test.raw)
		if err != nil {
			t.Errorf("expression %+v failed: %w", test.raw, err)
			t.Fail()
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
			t.Errorf("expecting %s to parse into %s, got %s instead", test.raw, test.expExpr, parsed)
		}
	}
}

func stringifySlice(exprs []*Expression) []string {
	if len(exprs) == 0 {
		return nil
	}
	ret := make([]string, 0, len(exprs))
	for _, expr := range exprs {
		ret = append(ret, expr.String())
	}
	return ret
}

func TestAggExpr(t *testing.T) {
	tests := []struct {
		raw      string
		expected []string
		err      error
	}{
		{"1", nil, nil},
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
		ress := stringifySlice(res)
		if !reflect.DeepEqual(ress, test.expected) {
			t.Errorf("expected %+v to have %+v as aggregating expressions, got %+v instead", test.raw, test.expected, ress)
		}
	}
}

// func (expr *Expression) UnmarshalJSON(data []byte) error {
// expr.stringer
// MarshalJSON
