package smda

import (
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
