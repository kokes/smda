package expr

import (
	"errors"
	"reflect"
	"testing"
)

func TestParsingContents(t *testing.T) {
	tests := []struct {
		raw     string
		expExpr Expression
	}{
		// standalone expressions and literals
		{"ahoy", &Identifier{Name: "ahoy"}},
		{"type", &Identifier{Name: "type"}},
		{"for", &Identifier{Name: "for"}},
		{"struct", &Identifier{Name: "struct"}},
		{"break", &Identifier{Name: "break"}},
		{"func", &Identifier{Name: "func"}},
		{"\"ahoy\"", &Identifier{Name: "ahoy"}},
		{"\"ahoy_world\"", &Identifier{Name: "ahoy_world"}},
		{"\"ahoy62\"", &Identifier{Name: "ahoy62"}},
		{"\"hello world\"", &Identifier{Name: "hello world", quoted: true}},
		{"254", &Integer{value: 254}},
		{"254.678", &Float{value: 254.678}},
		{"true", &Bool{value: true}},
		{"TRUE", &Bool{value: true}},
		{"True", &Bool{value: true}},
		{"false", &Bool{value: false}},
		{"FALSE", &Bool{value: false}},
		{"'foo'", &String{value: "foo"}},
		{"'foo bar'", &String{value: "foo bar"}},
		{"'foo'' bar'", &String{value: "foo' bar"}},
		{"null", &Null{}},
		{"NULL", &Null{}},
		{"NULl", &Null{}},
		{"*", &Identifier{Name: "*"}},

		// prefix operators
		{"-2", &Prefix{operator: tokenSub, right: &Integer{value: 2}}},
		{"-foo", &Prefix{operator: tokenSub, right: &Identifier{Name: "foo"}}},
		{"-\"Some column\"", &Prefix{operator: tokenSub, right: &Identifier{Name: "Some column", quoted: true}}},
		{"NOT foo", &Prefix{operator: tokenNot, right: &Identifier{Name: "foo"}}},
		{"NOT true", &Prefix{operator: tokenNot, right: &Bool{value: true}}},
		{"-(foo*bar)", &Prefix{operator: tokenSub, right: &Parentheses{
			inner: &Infix{
				left:     &Identifier{Name: "foo"},
				operator: tokenMul,
				right:    &Identifier{Name: "bar"},
			},
		}}},
		{"+2", &Prefix{operator: tokenAdd, right: &Integer{value: 2}}},
		{"+2.4", &Prefix{operator: tokenAdd, right: &Float{value: 2.4}}},

		// infix operators
		{"foo.bar", &Identifier{Namespace: &Identifier{Name: "foo"}, Name: "bar"}},
		{"foo.\"Bar\"", &Identifier{Namespace: &Identifier{Name: "foo"}, Name: "Bar", quoted: true}},
		{"\"Foo\".bar", &Identifier{Namespace: &Identifier{Name: "Foo", quoted: true}, Name: "bar"}},
		{"2 * foo.bar - 3", &Infix{operator: tokenSub, left: &Infix{
			operator: tokenMul,
			left:     &Integer{value: 2},
			right:    &Identifier{Namespace: &Identifier{Name: "foo"}, Name: "bar"},
		}, right: &Integer{value: 3}}},
		{"foo.*", &Identifier{Namespace: &Identifier{Name: "foo"}, Name: "*"}},
		{"4 * 2", &Infix{
			left:     &Integer{value: 4},
			operator: tokenMul,
			right:    &Integer{value: 2},
		}},
		{"4 + foo", &Infix{
			left:     &Integer{value: 4},
			operator: tokenAdd,
			right:    &Identifier{Name: "foo"},
		}},
		{"4 - foo", &Infix{
			left:     &Integer{value: 4},
			operator: tokenSub,
			right:    &Identifier{Name: "foo"},
		}},
		{"4 / foo", &Infix{
			left:     &Integer{value: 4},
			operator: tokenQuo,
			right:    &Identifier{Name: "foo"},
		}},
		{"4 + 3 + 2", &Infix{
			left: &Infix{
				left:     &Integer{value: 4},
				operator: tokenAdd,
				right:    &Integer{value: 3},
			},
			operator: tokenAdd,
			right:    &Integer{value: 2},
		}},
		{"4 + 3 * 2", &Infix{
			left:     &Integer{value: 4},
			operator: tokenAdd,
			right: &Infix{
				left:     &Integer{value: 3},
				operator: tokenMul,
				right:    &Integer{value: 2},
			},
		}},
		{"2 * \"ahoy\"", &Infix{operator: tokenMul,
			left:  &Integer{value: 2},
			right: &Identifier{Name: "ahoy"},
		}},
		{"foo / bar", &Infix{operator: tokenQuo,
			left:  &Identifier{Name: "foo"},
			right: &Identifier{Name: "bar"},
		}},
		{"2 * foo", &Infix{operator: tokenMul,
			left:  &Integer{value: 2},
			right: &Identifier{Name: "foo"},
		}},
		{"2 + 3*4", &Infix{operator: tokenAdd,
			left: &Integer{value: 2},
			right: &Infix{operator: tokenMul,
				left:  &Integer{value: 3},
				right: &Integer{value: 4},
			},
		}},
		{"foo like '%ahoy%'", &Infix{operator: tokenLike,
			left:  &Identifier{Name: "foo"},
			right: &String{value: "%ahoy%"},
		}},
		{"foo not like '%ahoy%'", &Prefix{operator: tokenNot,
			right: &Infix{operator: tokenLike,
				left:  &Identifier{Name: "foo"},
				right: &String{value: "%ahoy%"},
			},
		}},
		{"foo ilike '%ahoy%'", &Infix{operator: tokenIlike,
			left:  &Identifier{Name: "foo"},
			right: &String{value: "%ahoy%"},
		}},
		{"foo not ilike '%ahoy%'", &Prefix{operator: tokenNot,
			right: &Infix{operator: tokenIlike,
				left:  &Identifier{Name: "foo"},
				right: &String{value: "%ahoy%"},
			},
		}},

		// prefix and infix
		{"-4 / foo", &Infix{operator: tokenQuo,
			left:  &Prefix{operator: tokenSub, right: &Integer{value: 4}},
			right: &Identifier{Name: "foo"},
		}},
		{"foo in (1, 2)", &Infix{operator: tokenIn,
			left: &Identifier{Name: "foo"},
			right: &Tuple{inner: []Expression{
				&Integer{value: 1},
				&Integer{value: 2},
			}},
		}},
		{"foo in (1, 2) = true", &Infix{operator: tokenEq,
			left: &Infix{operator: tokenIn,
				left: &Identifier{Name: "foo"},
				right: &Tuple{inner: []Expression{
					&Integer{value: 1},
					&Integer{value: 2},
				}},
			},
			right: &Bool{value: true},
		}},
		// here the NOT is hacked together a bit... (it's not `prefix` in this context)
		// ARCH: would it make more sense to have a NotIn token?
		{"foo not in (1, 2)", &Prefix{operator: tokenNot,
			right: &Infix{operator: tokenIn,
				left: &Identifier{Name: "foo"},
				right: &Tuple{inner: []Expression{
					&Integer{value: 1},
					&Integer{value: 2},
				}},
			},
		}},
		{"foo is not in (1, 2)", &Prefix{operator: tokenNot,
			right: &Infix{operator: tokenIn,
				left: &Identifier{Name: "foo"},
				right: &Tuple{inner: []Expression{
					&Integer{value: 1},
					&Integer{value: 2},
				}},
			},
		}},
		{"foo is not null", &Prefix{operator: tokenNot,
			right: &Infix{operator: tokenEq,
				left:  &Identifier{Name: "foo"},
				right: &Null{},
			},
		}},

		// operators
		{"4 + 3 > 5", &Infix{operator: tokenGt,
			left: &Infix{operator: tokenAdd,
				left:  &Integer{value: 4},
				right: &Integer{value: 3},
			},
			right: &Integer{value: 5},
		}},
		{"4 + 3 >= 5", &Infix{operator: tokenGte,
			left: &Infix{operator: tokenAdd,
				left:  &Integer{value: 4},
				right: &Integer{value: 3},
			},
			right: &Integer{value: 5},
		}},
		{"4 > 3 = true", &Infix{operator: tokenEq,
			left: &Infix{operator: tokenGt,
				left:  &Integer{value: 4},
				right: &Integer{value: 3},
			},
			right: &Bool{value: true},
		}},
		{"foo = 'bar'", &Infix{operator: tokenEq,
			left:  &Identifier{Name: "foo"},
			right: &String{value: "bar"},
		}},
		{"'bar' = foo", &Infix{operator: tokenEq,
			left:  &String{value: "bar"},
			right: &Identifier{Name: "foo"},
		}},
		{"3 != bak", &Infix{operator: tokenNeq,
			left:  &Integer{value: 3},
			right: &Identifier{Name: "bak"},
		}},
		{"bak = 'my_literal'", &Infix{operator: tokenEq,
			left:  &Identifier{Name: "bak"},
			right: &String{value: "my_literal"},
		}},
		{"bak = 'my_li''ter''al'", &Infix{operator: tokenEq,
			left:  &Identifier{Name: "bak"},
			right: &String{value: "my_li'ter'al"},
		}},
		{"foo = true", &Infix{operator: tokenEq,
			left:  &Identifier{Name: "foo"},
			right: &Bool{value: true},
		}},
		{"foo is true", &Infix{operator: tokenIs,
			left:  &Identifier{Name: "foo"},
			right: &Bool{value: true},
		}},
		{"foo is not true", &Prefix{operator: tokenNot,
			right: &Infix{operator: tokenEq,
				left:  &Identifier{Name: "foo"},
				right: &Bool{value: true},
			},
		}},

		// boolean operators
		{"foo and bar", &Infix{operator: tokenAnd,
			left:  &Identifier{Name: "foo"},
			right: &Identifier{Name: "bar"},
		}},
		{"4 > 3 AND 5 = 1", &Infix{operator: tokenAnd,
			left: &Infix{operator: tokenGt,
				left:  &Integer{value: 4},
				right: &Integer{value: 3},
			},
			right: &Infix{operator: tokenEq,
				left:  &Integer{value: 5},
				right: &Integer{value: 1},
			},
		}},
		{"foo = 2 AND 3 = bar", &Infix{operator: tokenAnd,
			left: &Infix{operator: tokenEq,
				left:  &Identifier{Name: "foo"},
				right: &Integer{value: 2},
			},
			right: &Infix{operator: tokenEq,
				left:  &Integer{value: 3},
				right: &Identifier{Name: "bar"},
			},
		}},
		{"foo > 3 OR -2 <= bar", &Infix{operator: tokenOr,
			left: &Infix{operator: tokenGt,
				left:  &Identifier{Name: "foo"},
				right: &Integer{value: 3},
			},
			right: &Infix{operator: tokenLte,
				left:  &Prefix{operator: tokenSub, right: &Integer{value: 2}},
				right: &Identifier{Name: "bar"},
			},
		}},

		// parentheses
		{"2 * (4 + 3)", &Infix{operator: tokenMul,
			left: &Integer{value: 2},
			right: &Parentheses{
				inner: &Infix{operator: tokenAdd,
					left:  &Integer{value: 4},
					right: &Integer{value: 3},
				},
			},
		}},
		{"(4 + 3) - 2*3", &Infix{operator: tokenSub,
			left: &Parentheses{inner: &Infix{operator: tokenAdd,
				left:  &Integer{value: 4},
				right: &Integer{value: 3},
			},
			},
			right: &Infix{operator: tokenMul,
				left:  &Integer{value: 2},
				right: &Integer{value: 3},
			},
		}},
		{"2 * (1 - foo)", &Infix{operator: tokenMul,
			left: &Integer{value: 2},
			right: &Parentheses{
				&Infix{operator: tokenSub,
					left:  &Integer{value: 1},
					right: &Identifier{Name: "foo"},
				},
			},
		}},

		// just testing the parser, not checking the output
		{"foo = 'bar' AND bak = 'bar'", nil},
		{"1 < foo < 3", nil},
		{"bar < foo < bak", nil},

		{"sum(foo < 3)", nil},
		{"sum(foo >= 3)", nil},
		{"sum(foo <= 3)", nil},
		{"count()", &Function{name: "count"}},
		{"count(*)", &Function{name: "count"}},
		// TODO: make this work at some point - even in other places (e.g. `select foo, *, bar from ...`)
		//       think about ways to implement it without it being super hacky
		// {"count(*)", &Expression{etype: exprFunCall, value: "count"}},
		{"count(foobar)", &Function{name: "count", args: []Expression{
			&Identifier{Name: "foobar"},
		}}},
		{"count(distinct foobar)", &Function{name: "count", distinct: true, args: []Expression{
			&Identifier{Name: "foobar"},
		}}},
		{"count(distinct foo + bar)", &Function{name: "count", distinct: true, args: []Expression{
			&Infix{
				left:     &Identifier{Name: "foo"},
				operator: tokenAdd,
				right:    &Identifier{Name: "bar"},
			},
		}}},
		{"count(1, 2, 3)", &Function{name: "count", args: []Expression{
			&Integer{value: 1},
			&Integer{value: 2},
			&Integer{value: 3},
		}}},
		{"count(1, 2*3, 3)", &Function{name: "count", args: []Expression{
			&Integer{value: 1},
			&Infix{
				left:     &Integer{value: 2},
				operator: tokenMul,
				right:    &Integer{value: 3},
			},
			&Integer{value: 3},
		}}},
		// case insensitivity of function names
		{"COUNT(foobar)", &Function{name: "count", args: []Expression{
			&Identifier{Name: "foobar"},
		}}},
		{"Count(foobar)", &Function{name: "count", args: []Expression{
			&Identifier{Name: "foobar"},
		}}},
		{"counT(foobar)", &Function{name: "count", args: []Expression{
			&Identifier{Name: "foobar"},
		}}},
		{"coalesce(foo, bar, 1) - 4", &Infix{
			left: &Function{name: "coalesce", args: []Expression{
				&Identifier{Name: "foo"},
				&Identifier{Name: "bar"},
				&Integer{value: 1},
			}},
			operator: tokenSub,
			right:    &Integer{value: 4},
		}},
		{"nullif(baz, 'foo')", &Function{name: "nullif", args: []Expression{
			&Identifier{Name: "baz"},
			&String{value: "foo"},
		}}},
		{"nullif(bak, 103)", &Function{name: "nullif", args: []Expression{
			&Identifier{Name: "bak"},
			&Integer{value: 103},
		}}},
		{"round(1.234, 2)", &Function{name: "round", args: []Expression{
			&Float{value: 1.234},
			&Integer{value: 2},
		}}},
		{"count(foo = true)", &Function{name: "count", args: []Expression{
			&Infix{
				left:     &Identifier{Name: "foo"},
				operator: tokenEq,
				right:    &Bool{value: true},
			},
		}}},
		{"sum(foo > 3)", &Function{name: "sum", args: []Expression{
			&Infix{
				left:     &Identifier{Name: "foo"},
				operator: tokenGt,
				right:    &Integer{value: 3},
			},
		}}},
		{"foo as bar", &Relabel{
			inner: &Identifier{Name: "foo"},
			Label: "bar",
		}},
		{"foo bar", &Relabel{
			inner: &Identifier{Name: "foo"},
			Label: "bar",
		}},
		{"foo as Bar", &Relabel{
			inner: &Identifier{Name: "foo"},
			Label: "bar",
		}},
		{"foo Bar", &Relabel{
			inner: &Identifier{Name: "foo"},
			Label: "bar",
		}},
		{"foo as \"Bar\"", &Relabel{
			inner: &Identifier{Name: "foo"},
			Label: "Bar",
		}},
		{"foo \"Bar\"", &Relabel{
			inner: &Identifier{Name: "foo"},
			Label: "Bar",
		}},
		{"1+2 as bar", &Relabel{
			inner: &Infix{operator: tokenAdd,
				left:  &Integer{value: 1},
				right: &Integer{value: 2},
			},
			Label: "bar",
		}},
		{"1+2*3 as bar", &Relabel{
			inner: &Infix{operator: tokenAdd,
				left: &Integer{value: 1},
				right: &Infix{operator: tokenMul,
					left:  &Integer{value: 2},
					right: &Integer{value: 3},
				},
			},
			Label: "bar",
		}},
		{"1+2*3 bar", &Relabel{
			inner: &Infix{operator: tokenAdd,
				left: &Integer{value: 1},
				right: &Infix{operator: tokenMul,
					left:  &Integer{value: 2},
					right: &Integer{value: 3},
				},
			},
			Label: "bar",
		}},
	}

	for _, test := range tests {
		parsed, err := ParseStringExpr(test.raw)
		if err != nil {
			t.Errorf("expression %+v failed: %v", test.raw, err)
			continue
		}
		PruneFunctionCalls(parsed)
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
		{"", errEmptyExpression},
		{" ", errEmptyExpression},
		{"-", errUnsupportedPrefixToken},
		{"123123123131231231312312313123", errInvalidInteger},
		{"1e12312312323", errInvalidFloat},
		{"2 * (3-foo", errNoClosingBracket},
		{"foo + sum(bar", errNoClosingBracket},
		{"foo + sum(bar, ", errUnsupportedPrefixToken}, // ARCH: this is errNoClosingBracket, but we got to EOF first
		{"3 + 123(124)", errInvalidFunctionName},
		{"3 + \"Count\"(124)", errInvalidFunctionName},
		{"foo in bar", errInvalidTuple},
		{"foo not in bar", errInvalidTuple},
		{"foo in ()", errInvalidTuple},
		{"sin(distinct foo)", errDistinctInProjection},
		{"(@(", errUnsupportedPrefixToken}, // found via fuzzing; a weird error, I know
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
		{"*", []string{"*"}},
		{"*, foo, bar", []string{"*", "foo", "bar"}},
		{"foo, *, bar", []string{"foo", "*", "bar"}},
		{"foo, *", []string{"foo", "*"}},
		{"count(), foo, bar", []string{"count()", "foo", "bar"}},
		{"count(*), foo, bar", []string{"count()", "foo", "bar"}},
	}

testloop:
	for _, test := range tests {
		parsed, err := ParseStringExprs(test.list)
		if err != nil {
			t.Errorf("expression list %+v failed: %v", test.list, err)
			continue
		}
		for _, el := range parsed {
			PruneFunctionCalls(el)
		}

		var ip []Expression
		for _, expr := range test.individual {
			iparsed, err := ParseStringExpr(expr)
			if err != nil {
				t.Error(err)
				continue testloop
			}
			PruneFunctionCalls(iparsed)
			ip = append(ip, iparsed)
		}

		if !reflect.DeepEqual(parsed, ip) {
			t.Errorf("expecting %s to parse the same way as %s, got %v instead", test.list, test.individual, parsed)
		}
	}
}

func TestParsingSQL(t *testing.T) {
	tests := []struct {
		raw string
		err error
	}{
		{"WITH foo", errSQLOnlySelects},
		{"SELECT 1", nil},
		{"SELECT 1 LIMIT 100", nil},
		{"SELECT 1 WHERE TRUE", nil},
		{"SELECT 1 WHERE foo>3", nil},
		{"SELECT foo", nil},
		{"SELECT 'foo'", nil},
		{"SELECT 1+2*3", nil},
		// data-less functions
		{"SELECT now()", nil},
		{"SELECT version()", nil},

		{"SELECT foo FROM bar", nil},
		{"SELECT count(DISTINCT foo) FROM bar", nil},
		{"SELECT sum(DISTINCT foo), count(DISTINCT baz) FROM bar", nil},
		{"SELECT count(distinct) FROM bar", errDistinctNeedsColumn},
		{"SELECT * FROM bar", nil},
		{"SELECT *, foo FROM bar", nil},
		{"SELECT foo, * FROM bar", nil},
		{"SELECT foo, *, foo FROM bar", nil},
		{"SELECT foo, bar.*, foo FROM bar", nil},
		{"SELECT * FROM bar AS foo", nil},
		{"SELECT foo.* FROM bar AS foo", nil},
		{"SELECT * FROM bar AS \"Foo\"", nil},
		{"SELECT foo FROM bar@v020485a2686b8d38fe WHERE foo>2", nil},
		{"SELECT foo FROM bar WHERE 1=1 AND foo>bar", nil},
		{"SELECT foo FROM bar WHERE 1=1 AND foo>bar GROUP BY foo", nil},
		{"SELECT foo FROM bar GROUP BY foo", nil},
		{"SELECT foo FROM bar GROUP BY foo LIMIT 2", nil},
		{"SELECT foo FROM bar@v020485a2686b8d38fe LIMIT 200", nil},
		{"SELECT foo FROM bar GROUP BY foo ORDER BY foo, bar", nil},
		// we do roundtrips only, so we have to specify the full `ASC NULLS LAST`, we cannot have just `ASC`
		// TODO: this means we can't test parsing `ORDER BY foo NULLS LAST` with ASC being implicit
		// TODO(next): doing roundtrips also means we can't test comments - `{"SELECT * FROM bar\n-- my comment\nLIMIT 5", nil},`
		{"SELECT foo FROM bar GROUP BY foo ORDER BY foo ASC NULLS LAST, bar", nil},
		{"SELECT foo FROM bar GROUP BY foo ORDER BY foo ASC NULLS LAST, bar DESC NULLS FIRST", nil},
		{"SELECT foo FROM bar GROUP BY foo ORDER BY foo ASC NULLS FIRST, bar DESC NULLS FIRST", nil},
		{"SELECT foo FROM bar GROUP BY foo ORDER BY foo ASC NULLS LAST, bar DESC NULLS FIRST", nil},
		{"SELECT foo FROM bar GROUP BY foo ORDER BY foo ASC NULLS LAST, bar DESC NULLS FIRST LIMIT 3", nil},

		// GROUP BY number
		{"SELECT foo FROM bar GROUP BY 1", nil},
		{"SELECT foo, baz FROM bar GROUP BY 1, 2", nil},
		// ARCH: all those NULLS LAST/FIRST are due to roundtrip testing
		{"SELECT foo, baz FROM bar ORDER BY 1 ASC NULLS LAST", nil},
		{"SELECT foo, baz FROM bar ORDER BY 1 ASC NULLS LAST, 2 ASC NULLS LAST", nil},
		{"SELECT foo, baz FROM bar ORDER BY 1 DESC NULLS FIRST, 2 ASC NULLS LAST", nil},
		{"SELECT foo, baz FROM bar ORDER BY 1 ASC NULLS LAST, 2 DESC NULLS LAST", nil},

		{"SELECT foo FROM bar@234", errInvalidQuery},
		{"SELECT foo FROM bar GROUP for 1", errInvalidQuery},
		{"SELECT foo FROM bar GROUP BY foo LIMIT foo", errInvalidQuery},
		{"SELECT foo FROM bar GROUP BY foo ORDER on foo", errInvalidQuery},
		{"SELECT foo FROM bar GROUP BY foo ORDER BY foo NULLS LIMIT 100", errInvalidQuery},
		{"SELECT foo FROM bar GROUP BY foo ORDER BY foo NULLS BY LIMIT 100", errInvalidQuery},
		{"SELECT foo FROM bar GROUP BY foo ORDER BY foo ASC NULLS LIMIT 100", errInvalidQuery},
		{"SELECT foo FROM bar GROUP BY foo ORDER BY foo DESC NULLS LIMIT 100", errInvalidQuery},

		// fuzzing entries
		{"SELECT r FROM J@v111111D1110000000011", errInvalidDatasetVersion}, // this is invalid, because the version needs to be 18 chars
	}

	for _, test := range tests {
		parsed, err := ParseQuerySQL(test.raw)
		if !errors.Is(err, test.err) {
			t.Errorf("when parsing SQL query %v encountered %v, expected %v", test.raw, err, test.err)
			continue
		}
		if test.err == nil && parsed.String() != test.raw {
			t.Errorf("query %v failed our roundtrip test, got %s instead", test.raw, parsed.String())
		}
	}
}
