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
		{"!=*", []tokenType{tokenNeq, tokenMul}},
		{"*<>", []tokenType{tokenMul, tokenNeq}},
		{"*,*", []tokenType{tokenMul, tokenComma, tokenMul}},
	}

	for _, test := range tt {
		ts := NewTokenScanner([]byte(test.source))
		var tokens []tokenType
		for {
			token := ts.Scan()
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
	}

	for _, test := range tt {
		ts := NewTokenScanner([]byte(test.source))
		var tokens []token
		for {
			token := ts.Scan()
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
				token := ts.Scan()
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
