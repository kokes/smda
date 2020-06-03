// major TODOs:
// - error handling (just panics now)
// - stringer
// - sad paths in tests
// methods: isOperator, isLiteral, isKeyword etc.
// isPrecedence: get inspired: https://golang.org/src/go/token/token.go?s=4316:4348#L253
//   - then build an expression parser with precedence built in
// - potentially: positions of errors (for very clear error handling)
package smda

import (
	"bytes"
	"strconv"
)

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
func (ts *tokenScanner) Scan() token {
	if ts.position >= len(ts.code) {
		return token{tokenEOF, nil}
	}
	char := ts.code[ts.position]
	switch char {
	// TODO: what about other utf whitespace? if we choose not to consider it as whitespace, test for it
	case ' ', '\t', '\n':
		ts.position++
		return ts.Scan()
	case ',':
		ts.position++
		return token{tokenComma, nil}
	case '+':
		ts.position++
		return token{tokenAdd, nil}
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
			return token{tokenComment, ret}
		}
		ts.position++
		return token{tokenSub, nil}
	case '*':
		ts.position++
		return token{tokenMul, nil}
	case '/':
		ts.position++
		return token{tokenQuo, nil}
	case '=':
		ts.position++
		return token{tokenEq, nil}
	case '(':
		ts.position++
		return token{tokenLparen, nil}
	case ')':
		ts.position++
		return token{tokenRparen, nil}
	case '>':
		next := ts.peek(2)
		if bytes.Equal(next, []byte(">=")) {
			ts.position += 2
			return token{tokenGte, nil}
		}
		ts.position++
		return token{tokenGt, nil}
	case '!':
		next := ts.peek(2)
		if bytes.Equal(next, []byte("!=")) {
			ts.position += 2
			return token{tokenNeq, nil}
		}
		// return token{tokenInvalid, nil}
		panic("unknown token") // TODO: improve error handling
	case '<':
		next := ts.peek(2)
		if bytes.Equal(next, []byte("<=")) {
			ts.position += 2
			return token{tokenLte, nil}
		}
		if bytes.Equal(next, []byte("<>")) {
			ts.position += 2
			return token{tokenNeq, nil}
		}
		ts.position++
		return token{tokenLt, nil}
	case '.', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		// TODO: well, test this thoroughly (especially the sad paths)
		// TODO: refactor into a method, that returns (token, error), just like consumeStringLiteral
		//       this could also advance the position
		floatChars := []byte("0123456789e.-") // the minus here is not a unary minus, but for exponents - e.g. 2e-12
		intChars := []byte("0123456789")

		floatCandidate := sliceUntil(ts.code[ts.position:], floatChars)
		intCandidate := sliceUntil(ts.code[ts.position:], intChars)
		if len(intCandidate) == len(floatCandidate) {
			if _, err := strconv.ParseInt(string(intCandidate), 10, 64); err != nil {
				panic(err) // TODO: improve error handling (but make sure to return!)
			}
			ts.position += len(intCandidate)
			return token{tokenLiteralInt, intCandidate}
		}
		if _, err := strconv.ParseFloat(string(floatCandidate), 64); err != nil {
			panic(err) // TODO: improve error handling
		}
		ts.position += len(floatCandidate)
		return token{tokenLiteralFloat, floatCandidate}
	case '\'': // string literal
		return ts.consumeStringLiteral()
	case '"': // quoted identifier
		// TODO: move all this logic to consumeIdentifier? (peek first, see if it starts with a quote etc.)
		ts.position++
		ident := ts.consumeIdentifier()
		next := ts.peek(1)
		if !bytes.Equal(next, []byte("\"")) {
			panic("a quoted identifier has to end with a quotation mark") // TODO: improve err handling (+ add context - what did we find instead?)
		}
		ts.position++ // this is for the endquote
		ident.ttype = tokenIdentifierQuoted
		return ident
	default:
		return ts.consumeIdentifier()
	}
}

func (ts *tokenScanner) consumeIdentifier() token {
	// OPTIM: use a function with inequalities instead of this linear scan
	// TODO: make sure we restrict columns to be this format
	identChars := []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_0123456789")
	identCandidate := sliceUntil(ts.code[ts.position:], identChars)
	if len(identCandidate) == 0 {
		// TODO: this is not quite the right error (it would be if we were quoted)
		// this actually means there's nothing identifier-like here
		panic("cannot have an empty identifier") // TODO: improve err handling
	}
	ts.position += len(identCandidate)
	return token{tokenIdentifier, identCandidate}
}

const apostrophe = '\''

func (ts *tokenScanner) consumeStringLiteral() token {
	tok := token{tokenLiteralString, nil}
	tok.value = make([]byte, 0)
	for {
		idx := bytes.IndexByte(ts.code[ts.position+1:], apostrophe)
		if idx == -1 {
			panic("unmatched string ending") // TODO: improve error handling
		}
		chunk := ts.code[ts.position+1 : ts.position+idx+1]
		if bytes.IndexByte(chunk, '\n') > -1 {
			panic("cannot have a newline in a string literal")
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
	return tok
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
