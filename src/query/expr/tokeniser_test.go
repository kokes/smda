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
		for j, token := range tokens {
			if token.value != nil {
				t.Errorf("token %+v in %+v should not have a value, got %+v", token.ttype, test.source, token.value)
			}
			if token.ttype != test.expected[j] {
				t.Errorf("expecting %+v. token to be of type %+v, got %+v instead", j+1, test.expected[j], token.ttype)
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
		expected tokList
	}{
		{"/--", []tok{{tokenQuo, nil}, {tokenComment, []byte("")}}},
		{"/-- ", []tok{{tokenQuo, nil}, {tokenComment, []byte(" ")}}},
		{"/-- ahoy\n*", []tok{{tokenQuo, nil}, {tokenComment, []byte(" ahoy")}, {tokenMul, nil}}},
		{"2.34", []tok{{tokenLiteralFloat, []byte("2.34")}}},
		{"2.34e12", []tok{{tokenLiteralFloat, []byte("2.34e12")}}},
		{".5", []tok{{tokenLiteralFloat, []byte(".5")}}},
		{"1e3", []tok{{tokenLiteralFloat, []byte("1e3")}}}, // could be represented by an integer, but scientific notation implies float
		{".5e3", []tok{{tokenLiteralFloat, []byte(".5e3")}}},
		{".5e-3", []tok{{tokenLiteralFloat, []byte(".5e-3")}}},
		{"1-3", []tok{{tokenLiteralInt, []byte("1")}, {tokenSub, nil}, {tokenLiteralInt, []byte("3")}}},
		{"234", []tok{{tokenLiteralInt, []byte("234")}}},
		{"1232349000", []tok{{tokenLiteralInt, []byte("1232349000")}}},
		{"234*3", []tok{{tokenLiteralInt, []byte("234")}, {tokenMul, nil}, {tokenLiteralInt, []byte("3")}}},
		{"234*3", []tok{{tokenLiteralInt, []byte("234")}, {tokenMul, nil}, {tokenLiteralInt, []byte("3")}}},
		{"234\n\t*\n\t3", []tok{{tokenLiteralInt, []byte("234")}, {tokenMul, nil}, {tokenLiteralInt, []byte("3")}}},
		{"2.3e2 * 3e12", []tok{{tokenLiteralFloat, []byte("2.3e2")}, {tokenMul, nil}, {tokenLiteralFloat, []byte("3e12")}}},
		{"2.3e2 + 3e12", []tok{{tokenLiteralFloat, []byte("2.3e2")}, {tokenAdd, nil}, {tokenLiteralFloat, []byte("3e12")}}},
		{"2.3e2 - 3e12", []tok{{tokenLiteralFloat, []byte("2.3e2")}, {tokenSub, nil}, {tokenLiteralFloat, []byte("3e12")}}},
		{"''", []tok{{tokenLiteralString, []byte("")}}},
		{"'ahoy'*", []tok{{tokenLiteralString, []byte("ahoy")}, {tokenMul, nil}}},
		{"''''*", []tok{{tokenLiteralString, []byte("'")}, {tokenMul, nil}}},
		{"''''''*", []tok{{tokenLiteralString, []byte("''")}, {tokenMul, nil}}},
		{"'ah''oy'*", []tok{{tokenLiteralString, []byte("ah'oy")}, {tokenMul, nil}}},
		{"'ah''''oy'*", []tok{{tokenLiteralString, []byte("ah''oy")}, {tokenMul, nil}}},
		{"'ah''''''oy'*", []tok{{tokenLiteralString, []byte("ah'''oy")}, {tokenMul, nil}}},
		{"'ah'' '' '' '' ''oy'*", []tok{{tokenLiteralString, []byte("ah' ' ' ' 'oy")}, {tokenMul, nil}}},
		{"ahoy", []tok{{tokenIdentifier, []byte("ahoy")}}},
		{"hello_world", []tok{{tokenIdentifier, []byte("hello_world")}}},
		{"\"ahoy\"", []tok{{tokenIdentifierQuoted, []byte("ahoy")}}},
		{"+\"ahoy\"+", []tok{{tokenAdd, nil}, {tokenIdentifierQuoted, []byte("ahoy")}, {tokenAdd, nil}}},
		{"-- here is my comment\n1", []tok{{tokenComment, []byte(" here is my comment")}, {tokenLiteralInt, []byte("1")}}},
		{"--here is my comment\n1", []tok{{tokenComment, []byte("here is my comment")}, {tokenLiteralInt, []byte("1")}}},
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
	}

	for _, test := range tt {
		var first tokList
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
		{"foo", "foo"},
		{"foo-bar*2", "foo - bar * 2"},
		{"Foo+Bar", "Foo + Bar"},
		{"coalesce(1,2,3)", "coalesce ( 1 , 2 , 3 )"},
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
