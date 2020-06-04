// major TODOs:
// - stringer
// methods: isOperator, isLiteral, isKeyword etc.
// isPrecedence: get inspired: https://golang.org/src/go/token/token.go?s=4316:4348#L253
//   - then build an expression parser with precedence built in
// - potentially: positions of errors (for very clear error handling)
package smda

import (
	"bytes"
	"errors"
	"strconv"
)

var errUnknownToken = errors.New("unknown token")
var errInvalidInteger = errors.New("invalid integer")
var errInvalidFloat = errors.New("invalid floating point number")
var errInvalidString = errors.New("invalid string literal")
var errInvalidIdentifier = errors.New("invalid identifier")

type tokenType uint8
type token struct {
	ttype tokenType
	value []byte
}

// TODO: stringer
const (
	tokenInvalid tokenType = iota
	tokenIdentifier
	tokenIdentifierQuoted
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
	tokenLiteralInt
	tokenLiteralFloat
	tokenLiteralString
	tokenEOF // to signify end of parsing
	// potential additions: || (string concatenation), :: (casting), &|^ (bitwise operations), ** (power)
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
// TODO: it'd be nice to have a nice error reporting mechanism (we'll need positions to be embedded in the errors for that)
func (ts *tokenScanner) Scan() (token, error) {
	if ts.position >= len(ts.code) {
		return token{tokenEOF, nil}, nil
	}
	char := ts.code[ts.position]
	switch char {
	// TODO: what about other utf whitespace? if we choose not to consider it as whitespace, test for it
	case ' ', '\t', '\n':
		ts.position++
		return ts.Scan()
	case ',':
		ts.position++
		return token{tokenComma, nil}, nil
	case '+':
		ts.position++
		return token{tokenAdd, nil}, nil
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
			return token{tokenComment, ret}, nil
		}
		ts.position++
		return token{tokenSub, nil}, nil
	case '*':
		ts.position++
		return token{tokenMul, nil}, nil
	case '/':
		ts.position++
		return token{tokenQuo, nil}, nil
	case '=':
		next := ts.peek(2)
		if bytes.Equal(next, []byte("==")) {
			ts.position++
			return token{}, errUnknownToken
		}
		ts.position++
		return token{tokenEq, nil}, nil
	case '(':
		ts.position++
		return token{tokenLparen, nil}, nil
	case ')':
		ts.position++
		return token{tokenRparen, nil}, nil
	case '>':
		next := ts.peek(2)
		if bytes.Equal(next, []byte(">=")) {
			ts.position += 2
			return token{tokenGte, nil}, nil
		}
		ts.position++
		return token{tokenGt, nil}, nil
	case '!':
		next := ts.peek(2)
		if bytes.Equal(next, []byte("!=")) {
			ts.position += 2
			return token{tokenNeq, nil}, nil
		}
		ts.position++ // we need to advance the position, so that we don't get stuck
		return token{}, errUnknownToken
	case '<':
		next := ts.peek(2)
		if bytes.Equal(next, []byte("<=")) {
			ts.position += 2
			return token{tokenLte, nil}, nil
		}
		if bytes.Equal(next, []byte("<>")) {
			ts.position += 2
			return token{tokenNeq, nil}, nil
		}
		ts.position++
		return token{tokenLt, nil}, nil
	case '.', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		// TODO: well, test this thoroughly (especially the sad paths)
		// TODO: refactor into a method, that returns (token, error), just like consumeStringLiteral
		//       this could also advance the position
		floatChars := []byte("0123456789e.-") // the minus here is not a unary minus, but for exponents - e.g. 2e-12
		intChars := []byte("0123456789")

		floatCandidate := sliceUntil(ts.code[ts.position:], floatChars)
		intCandidate := sliceUntil(ts.code[ts.position:], intChars)
		if len(intCandidate) == len(floatCandidate) {
			ts.position += len(intCandidate)
			if _, err := strconv.ParseInt(string(intCandidate), 10, 64); err != nil {
				return token{}, errInvalidInteger
			}
			return token{tokenLiteralInt, intCandidate}, nil
		}
		ts.position += len(floatCandidate)
		if _, err := strconv.ParseFloat(string(floatCandidate), 64); err != nil {
			return token{}, errInvalidFloat
		}
		return token{tokenLiteralFloat, floatCandidate}, nil
	case '\'': // string literal
		return ts.consumeStringLiteral()
	case '"': // quoted identifier
		// TODO: move all this logic to consumeIdentifier? (peek first, see if it starts with a quote etc.)
		ts.position++
		ident, err := ts.consumeIdentifier()
		if err != nil {
			return token{}, err
		}
		next := ts.peek(1)
		ts.position++ // this is for the endquote
		if !bytes.Equal(next, []byte("\"")) {
			return token{}, errInvalidIdentifier
		}
		ident.ttype = tokenIdentifierQuoted
		return ident, nil
	default:
		return ts.consumeIdentifier()
	}
}

func (ts *tokenScanner) consumeIdentifier() (token, error) {
	// OPTIM: use a function with inequalities instead of this linear scan
	// TODO: make sure we restrict columns to be this format
	identChars := []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_0123456789")
	identCandidate := sliceUntil(ts.code[ts.position:], identChars)
	ts.position += len(identCandidate)
	if len(identCandidate) == 0 {
		// TODO: this is not quite the right error (it would be if we were quoted)
		// this actually means there's nothing identifier-like here
		ts.position++
		return token{}, errInvalidIdentifier
	}
	return token{tokenIdentifier, identCandidate}, nil
}

const apostrophe = '\''

func (ts *tokenScanner) consumeStringLiteral() (token, error) {
	tok := token{tokenLiteralString, nil}
	tok.value = make([]byte, 0)
	for {
		idx := bytes.IndexByte(ts.code[ts.position+1:], apostrophe)
		if idx == -1 {
			ts.position++
			return token{}, errInvalidString
		}
		chunk := ts.code[ts.position+1 : ts.position+idx+1]
		if bytes.IndexByte(chunk, '\n') > -1 {
			ts.position++
			return token{}, errInvalidString // TODO: add context: cannot have a newline there
		}
		tok.value = append(tok.value, chunk...)
		ts.position += idx + 1
		next := ts.peek(2)
		// apostrophes can be in string literals, but they need to be escaped by another apostrophe
		if bytes.Equal(next, []byte("''")) {
			ts.position++
			tok.value = append(tok.value, apostrophe)
		} else {
			break
		}
	}

	ts.position++
	return tok, nil
}

// slice a given input as long as all the bytes are within the chars slice
// e.g. ("foobar", "of") would yield "foo"
// it's linearly scanning chars each time, wasteful, but shouldn't matter for such small inputs we're using
func sliceUntil(s []byte, chars []byte) []byte {
	for j, c := range s {
		if bytes.IndexByte(chars, c) == -1 {
			return s[:j]
		}
	}
	return s
}
