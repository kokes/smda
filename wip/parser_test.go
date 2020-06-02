package smda

import (
	"bytes"
	"reflect"
	"testing"
)

func TestBasicTokenisation(t *testing.T) {
	tt := []struct {
		source   string
		expected []token
	}{
		{"", nil},
		{" ", nil},
		{"*/", []token{tokenMul, tokenQuo}},
		{"()", []token{tokenLparen, tokenRparen}},
		{">", []token{tokenGt}},
		{">=", []token{tokenGte}},
		{"!=*", []token{tokenNeq, tokenMul}},
		{"*<>", []token{tokenMul, tokenNeq}},
		{"*,*", []token{tokenMul, tokenComma, tokenMul}},
	}

	for _, test := range tt {
		ts := NewTokenScanner([]byte(test.source))
		var tokens []token
		for {
			tok, body := ts.Scan()
			if body != nil {
				t.Errorf("token %v in %v should not have a body, got %v", tok, test.source, body)
			}
			if tok == tokenEOF {
				break
			}
			tokens = append(tokens, tok)
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
	type tokenWithValue struct {
		tok  token
		body []byte
	}
	tt := []struct {
		source   string
		expected []tokenWithValue
	}{
		{"/--", []tokenWithValue{{tokenQuo, nil}, {tokenComment, []byte("")}}},
		{"/-- ", []tokenWithValue{{tokenQuo, nil}, {tokenComment, []byte(" ")}}},
		{"/-- ahoy\n*", []tokenWithValue{{tokenQuo, nil}, {tokenComment, []byte(" ahoy")}, {tokenMul, nil}}},
		{"2.34", []tokenWithValue{{tokenLiteralFloat, []byte("2.34")}}},
		{"2.34e12", []tokenWithValue{{tokenLiteralFloat, []byte("2.34e12")}}},
		{".5", []tokenWithValue{{tokenLiteralFloat, []byte(".5")}}},
		{".5e3", []tokenWithValue{{tokenLiteralFloat, []byte(".5e3")}}},
		{".5e-3", []tokenWithValue{{tokenLiteralFloat, []byte(".5e-3")}}},
		{"234", []tokenWithValue{{tokenLiteralInt, []byte("234")}}},
		{"1232349000", []tokenWithValue{{tokenLiteralInt, []byte("1232349000")}}},
	}

	for _, test := range tt {
		ts := NewTokenScanner([]byte(test.source))
		var tokens []tokenWithValue
		for {
			tok, value := ts.Scan()
			if tok == tokenEOF {
				break
			}
			tokens = append(tokens, tokenWithValue{tok, value})
		}

		if !reflect.DeepEqual(tokens, test.expected) {
			t.Errorf("expected %v to tokenise as %v, got %v", test.source, test.expected, tokens)
		}
	}
}
