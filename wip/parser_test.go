package smda

import (
	"reflect"
	"testing"
)

// TODO: cannot return errors??
func tokenise(source []byte) ([]token, error) {
	ts := NewTokenScanner(source)
	var tokens []token
	for {
		tok, _ := ts.Scan()
		if tok == tokenEOF {
			return tokens, nil
		}
		tokens = append(tokens, tok)
	}
}

func TestBasicTokenisation(t *testing.T) {
	tt := []struct {
		source   string
		expected []token
	}{
		{"", nil},
		{"*/", []token{tokenMul, tokenQuo}},
		{"()", []token{tokenLparen, tokenRparen}},
	}

	for _, test := range tt {
		tokens, err := tokenise([]byte(test.source))
		if err != nil {
			t.Errorf("could not tokenise %v", test.source)
			continue
		}
		if !reflect.DeepEqual(tokens, test.expected) {
			t.Errorf("expected %v to tokenise as %v, got %v", test.source, test.expected, tokens)
		}
	}

}
