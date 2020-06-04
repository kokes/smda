package smda

import (
	"bytes"
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
		ts := NewTokenScanner([]byte(test.source))
		var tokens []tokenType
		for {
			token, err := ts.Scan()
			if err != nil {
				t.Fatal(err)
			}
			if token.value != nil {
				t.Errorf("token %v in %v should not have a value, got %v", token.ttype, test.source, token.value)
			}
			if token.ttype == tokenEOF {
				break
			}
			tokens = append(tokens, token.ttype)
		}

		if !reflect.DeepEqual(tokens, test.expected) {
			t.Errorf("expected %v to tokenise as %v, got %v", test.source, test.expected, tokens)
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
			t.Errorf("expecting to slice %v into %v using %v, but got %v", test.input, test.output, test.chars, string(output))
		}
	}
}

func TestTokenisationWithValues(t *testing.T) {
	tt := []struct {
		source   string
		expected []token
	}{
		{"/--", []token{{tokenQuo, nil}, {tokenComment, []byte("")}}},
		{"/-- ", []token{{tokenQuo, nil}, {tokenComment, []byte(" ")}}},
		{"/-- ahoy\n*", []token{{tokenQuo, nil}, {tokenComment, []byte(" ahoy")}, {tokenMul, nil}}},
		{"2.34", []token{{tokenLiteralFloat, []byte("2.34")}}},
		{"2.34e12", []token{{tokenLiteralFloat, []byte("2.34e12")}}},
		{".5", []token{{tokenLiteralFloat, []byte(".5")}}},
		{".5e3", []token{{tokenLiteralFloat, []byte(".5e3")}}},
		{".5e-3", []token{{tokenLiteralFloat, []byte(".5e-3")}}},
		{"234", []token{{tokenLiteralInt, []byte("234")}}},
		{"1232349000", []token{{tokenLiteralInt, []byte("1232349000")}}},
		{"234*3", []token{{tokenLiteralInt, []byte("234")}, {tokenMul, nil}, {tokenLiteralInt, []byte("3")}}},
		{"234*3", []token{{tokenLiteralInt, []byte("234")}, {tokenMul, nil}, {tokenLiteralInt, []byte("3")}}},
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
		{"\"ahoy\"", []token{{tokenIdentifierQuoted, []byte("ahoy")}}},
		{"+\"ahoy\"+", []token{{tokenAdd, nil}, {tokenIdentifierQuoted, []byte("ahoy")}, {tokenAdd, nil}}},
	}

	for _, test := range tt {
		ts := NewTokenScanner([]byte(test.source))
		var tokens []token
		for {
			token, err := ts.Scan()
			if err != nil {
				t.Fatal(err)
			}
			if token.ttype == tokenEOF {
				break
			}
			tokens = append(tokens, token)
		}

		if !reflect.DeepEqual(tokens, test.expected) {
			t.Errorf("expected %v to tokenise as %v, got %v", test.source, test.expected, tokens)
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
		var first []token
		for j, source := range test {
			ts := NewTokenScanner([]byte(source))
			var tokens []token
			for {
				token, err := ts.Scan()
				if err != nil {
					t.Fatal(err)
				}
				if token.ttype == tokenEOF {
					break
				}
				tokens = append(tokens, token)
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
		source string
		errs   []error
	}{
		{"123 * 345", nil},
		{"123.3.3 * 345", []error{errInvalidFloat}},
		{"123 / 3453123121241241231231231231231", []error{errInvalidInteger}},
		{"ahoy'", []error{errInvalidString}},
		{"fooba$", []error{errInvalidIdentifier}},
		{"\"fooba$", []error{errInvalidIdentifier}},
		{"\"\"", []error{errInvalidIdentifier}},
		{"123 !! 456", []error{errUnknownToken, errUnknownToken}},
		{"2 == 3", []error{errUnknownToken}}, // we disallow == as an equality test, we use SQL's '='
		{"'some text\nother text'", []error{errInvalidString, errInvalidString}},
	}

	for _, test := range tt {
		ts := NewTokenScanner([]byte(test.source))
		var tokens []token
		var errs []error
		for {
			token, err := ts.Scan()
			if err != nil {
				errs = append(errs, err)
			}
			if token.ttype == tokenEOF {
				break
			}
			tokens = append(tokens, token)
		}
		// TODO: it's not best pratice to use DeepEqual with errors, fix
		if !reflect.DeepEqual(errs, test.errs) {
			t.Errorf("expecting %v to trigger %v, but got %v", test.source, test.errs, errs)
		}
	}
}
