package expr

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func TestBasicTokenisation(t *testing.T) {
	tt := []struct {
		source   string
		expected []tokenType
	}{
		{"", nil},
		{" ", nil},
		{"*/", []tokenType{tokenMul, tokenQuo}},
		{"()", []tokenType{tokenLparen, tokenRparen}},
		{">", []tokenType{tokenGt}},
		{">=", []tokenType{tokenGte}},
		{"<", []tokenType{tokenLt}},
		{"<=", []tokenType{tokenLte}},
		{"=", []tokenType{tokenEq}},
		{"!=*", []tokenType{tokenNeq, tokenMul}},
		{"*<>", []tokenType{tokenMul, tokenNeq}},
		{"*,*", []tokenType{tokenMul, tokenComma, tokenMul}},
		{"- -", []tokenType{tokenSub, tokenSub}},
	}

	for _, test := range tt {
		tokens, err := tokeniseString(test.source)
		if err != nil {
			t.Error(err)
			continue
		}
		if len(tokens) != len(test.expected) {
			t.Errorf("expected %+v tokens, got %+v", len(test.expected), len(tokens))
			continue
		}
		for j, tok := range tokens {
			if tok.value != nil {
				t.Errorf("token %+v in %+v should not have a value, got %+v", tok.ttype, test.source, tok.value)
			}
			if tok.ttype != test.expected[j] {
				t.Errorf("expecting %+v. token to be of type %+v, got %+v instead", j+1, test.expected[j], tok.ttype)
			}
		}
	}
}

func TestSlicingUntil(t *testing.T) {
	tt := []struct {
		input  string
		chars  string
		output string
	}{
		{"0123", "012345", "0123"},
		{"0.123", "012345", "0"},
		{"0.123f", "012345.", "0.123"},
		{"0.123e12", "012345.e", "0.123e12"},
		{"0.123e12e1e1", "012345.e", "0.123e12e1e1"},
	}
	for _, test := range tt {
		output := sliceUntil([]byte(test.input), []byte(test.chars))
		if !bytes.Equal(output, []byte(test.output)) {
			t.Errorf("expecting to slice %+v into %+v using %+v, but got %+v", test.input, test.output, test.chars, string(output))
		}
	}
}

func TestTokenisationWithValues(t *testing.T) {
	tt := []struct {
		source   string
		expected tokenList
	}{
		{"/--", []token{{tokenQuo, nil}, {tokenComment, []byte("")}}},
		{"/-- ", []token{{tokenQuo, nil}, {tokenComment, []byte(" ")}}},
		{"/-- ahoy\n*", []token{{tokenQuo, nil}, {tokenComment, []byte(" ahoy")}, {tokenMul, nil}}},
		{"2.34", []token{{tokenLiteralFloat, []byte("2.34")}}},
		{"2.34e12", []token{{tokenLiteralFloat, []byte("2.34e12")}}},
		{".5", []token{{tokenLiteralFloat, []byte(".5")}}},
		{"1e3", []token{{tokenLiteralFloat, []byte("1e3")}}}, // could be represented by an integer, but scientific notation implies float
		{".5e3", []token{{tokenLiteralFloat, []byte(".5e3")}}},
		{".5e-3", []token{{tokenLiteralFloat, []byte(".5e-3")}}},
		{"1-3", []token{{tokenLiteralInt, []byte("1")}, {tokenSub, nil}, {tokenLiteralInt, []byte("3")}}},
		{"234", []token{{tokenLiteralInt, []byte("234")}}},
		{"1232349000", []token{{tokenLiteralInt, []byte("1232349000")}}},
		{"234*3", []token{{tokenLiteralInt, []byte("234")}, {tokenMul, nil}, {tokenLiteralInt, []byte("3")}}},
		{"234*3", []token{{tokenLiteralInt, []byte("234")}, {tokenMul, nil}, {tokenLiteralInt, []byte("3")}}},
		{"distinct foo", []token{{tokenDistinct, nil}, {tokenIdentifier, []byte("foo")}}},
		{"DISTINCT foo", []token{{tokenDistinct, nil}, {tokenIdentifier, []byte("foo")}}},
		{"234\n\t*\n\t3", []token{{tokenLiteralInt, []byte("234")}, {tokenMul, nil}, {tokenLiteralInt, []byte("3")}}},
		{"2.3e2 * 3e12", []token{{tokenLiteralFloat, []byte("2.3e2")}, {tokenMul, nil}, {tokenLiteralFloat, []byte("3e12")}}},
		{"2.3e2 + 3e12", []token{{tokenLiteralFloat, []byte("2.3e2")}, {tokenAdd, nil}, {tokenLiteralFloat, []byte("3e12")}}},
		{"2.3e2 - 3e12", []token{{tokenLiteralFloat, []byte("2.3e2")}, {tokenSub, nil}, {tokenLiteralFloat, []byte("3e12")}}},
		{"''", []token{{tokenLiteralString, []byte("")}}},
		{"'ahoy'*", []token{{tokenLiteralString, []byte("ahoy")}, {tokenMul, nil}}},
		{"''''*", []token{{tokenLiteralString, []byte("'")}, {tokenMul, nil}}},
		{"''''''*", []token{{tokenLiteralString, []byte("''")}, {tokenMul, nil}}},
		{"'ah''oy'*", []token{{tokenLiteralString, []byte("ah'oy")}, {tokenMul, nil}}},
		{"'ah''''oy'*", []token{{tokenLiteralString, []byte("ah''oy")}, {tokenMul, nil}}},
		{"'ah''''''oy'*", []token{{tokenLiteralString, []byte("ah'''oy")}, {tokenMul, nil}}},
		{"'ah'' '' '' '' ''oy'*", []token{{tokenLiteralString, []byte("ah' ' ' ' 'oy")}, {tokenMul, nil}}},
		{"ahoy", []token{{tokenIdentifier, []byte("ahoy")}}},
		{"hello_world", []token{{tokenIdentifier, []byte("hello_world")}}},
		{"foo as bar", []token{{tokenIdentifier, []byte("foo")}, {tokenAs, nil}, {tokenIdentifier, []byte("bar")}}},
		{"\"ahoy\"", []token{{tokenIdentifierQuoted, []byte("ahoy")}}},
		{"foo in (1, 2)", []token{{tokenIdentifier, []byte("foo")}, {tokenIn, nil},
			{tokenLparen, nil}, {tokenLiteralInt, []byte("1")}, {tokenComma, nil}, {tokenLiteralInt, []byte("2")}, {tokenRparen, nil}}},
		{"foo not in (1, 2)", []token{{tokenIdentifier, []byte("foo")}, {tokenNot, nil}, {tokenIn, nil},
			{tokenLparen, nil}, {tokenLiteralInt, []byte("1")}, {tokenComma, nil}, {tokenLiteralInt, []byte("2")}, {tokenRparen, nil}}},
		// quoted keywords are identifiers, not keywords
		{"\"select\"", []token{{tokenIdentifierQuoted, []byte("select")}}},
		{"\"SELECT\"", []token{{tokenIdentifierQuoted, []byte("SELECT")}}},
		{"\"from\"", []token{{tokenIdentifierQuoted, []byte("from")}}},
		{"\"nulls\"", []token{{tokenIdentifierQuoted, []byte("nulls")}}},
		{"+\"ahoy\"+", []token{{tokenAdd, nil}, {tokenIdentifierQuoted, []byte("ahoy")}, {tokenAdd, nil}}},
		{"-- here is my comment\n1", []token{{tokenComment, []byte(" here is my comment")}, {tokenLiteralInt, []byte("1")}}},
		{"--here is my comment\n1", []token{{tokenComment, []byte("here is my comment")}, {tokenLiteralInt, []byte("1")}}},
		{"foo@v020485a2686b8d38fe", []token{{tokenIdentifier, []byte("foo")}, {tokenAt, nil}, {tokenIdentifier, []byte("v020485a2686b8d38fe")}}},
		{"select foo from bar", []token{{tokenSelect, nil}, {tokenIdentifier, []byte("foo")}, {tokenFrom, nil}, {tokenIdentifier, []byte("bar")}}},
		{"select foo, bar from baz", []token{{tokenSelect, nil}, {tokenIdentifier, []byte("foo")}, {tokenComma, nil}, {tokenIdentifier, []byte("bar")}, {tokenFrom, nil}, {tokenIdentifier, []byte("baz")}}},
		// we're experimenting with vID here
		{"select foo from bar@v020485a2686b8d38fe", []token{{tokenSelect, nil}, {tokenIdentifier, []byte("foo")}, {tokenFrom, nil}, {tokenIdentifier, []byte("bar")}, {tokenAt, nil},
			{tokenIdentifier, []byte("v020485a2686b8d38fe")}}},
		{"select foo from bar@v020485a2686b8d38fe where foo > 1", []token{{tokenSelect, nil}, {tokenIdentifier, []byte("foo")}, {tokenFrom, nil}, {tokenIdentifier, []byte("bar")}, {tokenAt, nil},
			{tokenIdentifier, []byte("v020485a2686b8d38fe")}, {tokenWhere, nil}, {tokenIdentifier, []byte("foo")}, {tokenGt, nil}, {tokenLiteralInt, []byte("1")},
		}},
		{"select foo from bar@v020485a2686b8d38fe group by foo, bar", []token{{tokenSelect, nil}, {tokenIdentifier, []byte("foo")}, {tokenFrom, nil}, {tokenIdentifier, []byte("bar")}, {tokenAt, nil},
			{tokenIdentifier, []byte("v020485a2686b8d38fe")}, {tokenGroup, nil}, {tokenBy, nil}, {tokenIdentifier, []byte("foo")}, {tokenComma, nil}, {tokenIdentifier, []byte("bar")},
		}},
		{"select foo from bar@v020485a2686b8d38fe limit 123", []token{{tokenSelect, nil}, {tokenIdentifier, []byte("foo")}, {tokenFrom, nil}, {tokenIdentifier, []byte("bar")}, {tokenAt, nil},
			{tokenIdentifier, []byte("v020485a2686b8d38fe")}, {tokenLimit, nil}, {tokenLiteralInt, []byte("123")},
		}},
		{"select foo from bar order by foo", []token{{tokenSelect, nil}, {tokenIdentifier, []byte("foo")}, {tokenFrom, nil}, {tokenIdentifier, []byte("bar")},
			{tokenOrder, nil}, {tokenBy, nil}, {tokenIdentifier, []byte("foo")},
		}},
		{"select foo from bar order by foo asc", []token{{tokenSelect, nil}, {tokenIdentifier, []byte("foo")}, {tokenFrom, nil}, {tokenIdentifier, []byte("bar")},
			{tokenOrder, nil}, {tokenBy, nil}, {tokenIdentifier, []byte("foo")}, {tokenAsc, nil},
		}},
		{"select foo from bar order by foo desc", []token{{tokenSelect, nil}, {tokenIdentifier, []byte("foo")}, {tokenFrom, nil}, {tokenIdentifier, []byte("bar")},
			{tokenOrder, nil}, {tokenBy, nil}, {tokenIdentifier, []byte("foo")}, {tokenDesc, nil},
		}},
		{"select foo from bar order by foo desc nulls first", []token{{tokenSelect, nil}, {tokenIdentifier, []byte("foo")}, {tokenFrom, nil}, {tokenIdentifier, []byte("bar")},
			{tokenOrder, nil}, {tokenBy, nil}, {tokenIdentifier, []byte("foo")}, {tokenDesc, nil}, {tokenNulls, nil}, {tokenFirst, nil},
		}},
		{"select foo from bar order by foo desc nulls last", []token{{tokenSelect, nil}, {tokenIdentifier, []byte("foo")}, {tokenFrom, nil}, {tokenIdentifier, []byte("bar")},
			{tokenOrder, nil}, {tokenBy, nil}, {tokenIdentifier, []byte("foo")}, {tokenDesc, nil}, {tokenNulls, nil}, {tokenLast, nil},
		}},
		{"select foo from bar order by foo nulls last", []token{{tokenSelect, nil}, {tokenIdentifier, []byte("foo")}, {tokenFrom, nil}, {tokenIdentifier, []byte("bar")},
			{tokenOrder, nil}, {tokenBy, nil}, {tokenIdentifier, []byte("foo")}, {tokenNulls, nil}, {tokenLast, nil},
		}},
	}

	for _, test := range tt {
		tokens, err := tokeniseString(test.source)
		if err != nil {
			t.Errorf("failed to tokenise %+v, got %+v", test.source, err)
			continue
		}
		if !reflect.DeepEqual(tokens, test.expected) {
			t.Errorf("expected %+v to tokenise as %+v, got %+v", test.source, test.expected, tokens)
		}
	}
}

func TestTokenisationInvariants(t *testing.T) {
	tt := [][]string{
		{"2*3", "2 * 3", "2\t*\t\t\t\t\n3\t\n"},
		{"2\t/1234", "2/1234"},
		{"2\n3", "2 3"},
		{"", "\t\n "},
		{"foo is bar", "foo Is bar"},
		{"foo is bar", "foo IS bar"},
		{"foo is NUll", "foo IS NULL"},
	}

	for _, test := range tt {
		var first tokenList
		for j, source := range test {
			tokens, err := tokeniseString(source)
			if err != nil {
				t.Fatal(err)
			}
			if j == 0 {
				first = tokens
				continue
			}
			if !reflect.DeepEqual(tokens, first) {
				t.Errorf("expected %q to tokenise as %q, got %q", source, first, tokens)
			}
		}
	}
}

func TestTokenisationErrors(t *testing.T) {
	tt := []struct {
		source   string
		firstErr error
	}{
		{"123 * 345", nil},
		// this now tokenises just fine, because it doesn't detect a boundary between 123.3 and .3
		// {"123.3.3 * 345", errInvalidFloat},
		{"123 / 3453123121241241231231231231231", errInvalidInteger},
		{"ahoy'", errInvalidString},
		{"\"hello world", errInvalidIdentifier},
		{"\"hello\nworld\"", errInvalidIdentifier},
		{"fooba$", errInvalidIdentifier},
		{"\"fooba$", errInvalidIdentifier},
		{"\"\"", errInvalidIdentifier},
		{"123 !! 456", errUnknownToken},
		{"foo && bar", errInvalidIdentifier}, // again, not the right error
		{"foo || bar", errInvalidIdentifier},
		{"2 == 3", errUnknownToken}, // we disallow == as an equality test, we use SQL's '='
		{"'some text\nother text'", errInvalidString},
		// we don't consider nbsp as whitespace (the error isn't the best, but at least it errs)
		{"1 =\xa01", errInvalidIdentifier},
	}

	for _, test := range tt {
		_, err := tokeniseString(test.source)
		if !errors.Is(err, test.firstErr) {
			fmt.Println("GOT")
			fmt.Println(tokeniseString(test.source))
			t.Errorf("expecting %+v when parsing %+v, got %+v instead", test.firstErr, test.source, err)
		}
	}
}

func TestTokenisationStringer(t *testing.T) {
	tests := []struct {
		source      string
		stringified string
	}{
		{"1 + 3/2", "1 + 3 / 2"},
		// ARCH: maybe consider not printing whitespace after `(`` and before `)` (also see the coalesce test)
		{" ( a+b) ", "( a + b )"},
		{"foo and bar", "foo AND bar"},
		{"foo as bar", "foo AS bar"},
		{"foo is true", "foo IS TRUE"},
		{"Bar is not False", "Bar IS NOT FALSE"},
		{"foo", "foo"},
		{"foo-bar*2", "foo - bar * 2"},
		{"Foo+Bar", "Foo + Bar"},
		{"coalesce(1,2,3)", "coalesce ( 1 , 2 , 3 )"},
		// {"count(distinct foo)", "COUNT(DISTINCT foo)"},
	}

	for _, test := range tests {
		parsed, err := tokeniseString(test.source)
		if err != nil {
			t.Error(err)
			continue
		}
		if parsed.String() != test.stringified {
			t.Errorf("expecting expression %v to be parsed and then stringified into %v, got %v instead", test.source, test.stringified, parsed.String())
		}
	}
}
