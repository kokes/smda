package smda

import (
	"bytes"
)

type token uint8

// TODO: stringer
const (
	tokenInvalid token = iota
	// tokenIdentifier
	tokenComment
	tokenAdd
	tokenSub
	tokenMul
	tokenQuo
	tokenEq
	tokenNeq
	tokenGt
	tokenLt
	tokenGte
	tokenLte
	tokenLparen
	tokenRparen
	tokenComma
	// tokenLiteralInt
	// tokenLiteralFloat
	// tokenLiteralString
	// tokenLiteralBool
	tokenEOF // to signify end of parsing
)

type tokenScanner struct {
	code     []byte
	position int
}

func NewTokenScanner(s []byte) *tokenScanner {
	return &tokenScanner{
		code: s,
	}
}

func (ts *tokenScanner) peek(n int) []byte {
	ret := make([]byte, n)
	newpos := ts.position + n

	if newpos > len(ts.code) {
		newpos = len(ts.code)
	}
	copy(ret, ts.code[ts.position:newpos])
	return ret
}

// TODO: check coverage of the switch statement
func (ts *tokenScanner) Scan() (tok token, value []byte) {
	if ts.position >= len(ts.code) {
		return tokenEOF, nil
	}
	char := ts.code[ts.position]
	switch char {
	// TODO: what about other utf whitespace? if we choose not to consider it as whitespace, test for it
	case ' ', '\t', '\n':
		ts.position++
		return ts.Scan()
	case '+':
		ts.position++
		return tokenAdd, nil
	case '-':
		next := ts.peek(2)
		if bytes.Equal(next, []byte("--")) {
			// we have a comment, everything up until the end of this line is its content
			newline := bytes.IndexByte(ts.code[ts.position:], '\n')
			endpos := ts.position + newline
			if newline == -1 {
				endpos = len(ts.code)
			}
			ret := ts.code[ts.position+2 : endpos]
			ts.position += endpos - ts.position + 1
			return tokenComment, ret
		}
		ts.position++
		return tokenSub, nil
	case '*':
		ts.position++
		return tokenMul, nil
	case '/':
		ts.position++
		return tokenQuo, nil
	case '=':
		ts.position++
		return tokenEq, nil
	case '(':
		ts.position++
		return tokenLparen, nil
	case ')':
		ts.position++
		return tokenRparen, nil
	case '>':
		next := ts.peek(2)
		if bytes.Equal(next, []byte(">=")) {
			ts.position += 2
			return tokenGte, nil
		}
		ts.position++
		return tokenGt, nil
	case '<':
		next := ts.peek(2)
		if bytes.Equal(next, []byte("<=")) {
			ts.position += 2
			return tokenLte, nil
		}
		if bytes.Equal(next, []byte("<>")) {
			ts.position += 2
			return tokenNeq, nil
		}
		ts.position++
		return tokenLt, nil
	default:
		panic("unknown token")
	}
}
