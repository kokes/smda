package expr

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var errUnknownToken = errors.New("unknown token")
var errInvalidInteger = errors.New("invalid integer")
var errInvalidFloat = errors.New("invalid floating point number")
var errInvalidString = errors.New("invalid string literal")
var errInvalidIdentifier = errors.New("invalid identifier")

type tokenType uint8
type tok struct {
	ttype tokenType
	value []byte
}
type tokList []tok

const (
	tokenInvalid tokenType = iota
	tokenIdentifier
	tokenIdentifierQuoted
	tokenComment
	tokenAnd
	tokenOr
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

func (tok tok) String() string {
	switch tok.ttype {
	case tokenIdentifier:
		return string(tok.value)
	case tokenIdentifierQuoted:
		return fmt.Sprintf("\"%s\"", tok.value)
	case tokenComment:
		return fmt.Sprintf("-- %v\n", tok.value)
	case tokenAnd:
		return "&&" // we might change this to AND (and || to OR) to do this SQL compatibility thing
	case tokenOr:
		return "||"
	case tokenAdd:
		return "+"
	case tokenSub:
		return "-"
	case tokenMul:
		return "*"
	case tokenQuo:
		return "/"
	case tokenEq:
		return "==" // TODO: this is a compatibility thing, will need to revert this to '=' eventually
	case tokenNeq:
		return "!="
	case tokenGt:
		return ">"
	case tokenLt:
		return "<"
	case tokenGte:
		return ">="
	case tokenLte:
		return "<="
	case tokenLparen:
		return "("
	case tokenRparen:
		return ")"
	case tokenComma:
		return ","
	case tokenLiteralInt:
		return string(tok.value)
	case tokenLiteralFloat:
		return string(tok.value)
	case tokenLiteralString:
		escaped := bytes.ReplaceAll(tok.value, []byte("'"), []byte("\\'"))
		return fmt.Sprintf("'%s'", escaped)
	default:
		panic(fmt.Sprintf("unknown token type: %v", tok.ttype))
	}
}

// TODO: this might need some tests, especially if we have successive identifiers (e.g. "foo bar baz")
func (tokens tokList) String() string {
	var sb strings.Builder
	for _, tok := range tokens {
		sb.WriteString(tok.String())
		sb.WriteByte(' ')
	}
	return sb.String()
}

type tokenScanner struct {
	code     []byte
	position int
}

func NewTokenScanner(s []byte) *tokenScanner {
	return &tokenScanner{
		code: s,
	}
}

func NewTokenScannerFromString(s string) *tokenScanner {
	return &tokenScanner{
		code: []byte(s),
	}
}

func TokeniseString(s string) (tokList, error) {
	scanner := NewTokenScannerFromString(s)
	var tokens []tok
	for {
		tok, err := scanner.Scan()
		if err != nil {
			return nil, err
		}
		if tok.ttype == tokenEOF {
			break
		}
		tokens = append(tokens, tok)
	}
	return tokens, nil
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

func (ts *tokenScanner) Scan() (tok, error) {
	if ts.position >= len(ts.code) {
		return tok{tokenEOF, nil}, nil
	}
	char := ts.code[ts.position]
	switch char {
	case ' ', '\t', '\n':
		ts.position++
		return ts.Scan()
	case ',':
		ts.position++
		return tok{tokenComma, nil}, nil
	case '&':
		next := ts.peek(2)
		if bytes.Equal(next, []byte("&&")) {
			ts.position += 2
			return tok{tokenAnd, nil}, nil
		}
		ts.position++
		return tok{}, errUnknownToken
	case '|':
		next := ts.peek(2)
		if bytes.Equal(next, []byte("||")) {
			ts.position += 2
			return tok{tokenOr, nil}, nil
		}
		ts.position++
		return tok{}, errUnknownToken
	case '+':
		ts.position++
		return tok{tokenAdd, nil}, nil
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
			return tok{tokenComment, ret}, nil
		}
		ts.position++
		return tok{tokenSub, nil}, nil
	case '*':
		ts.position++
		return tok{tokenMul, nil}, nil
	case '/':
		ts.position++
		return tok{tokenQuo, nil}, nil
	case '=':
		next := ts.peek(2)
		if bytes.Equal(next, []byte("==")) {
			ts.position++
			return tok{}, errUnknownToken
		}
		ts.position++
		return tok{tokenEq, nil}, nil
	case '(':
		ts.position++
		return tok{tokenLparen, nil}, nil
	case ')':
		ts.position++
		return tok{tokenRparen, nil}, nil
	case '>':
		next := ts.peek(2)
		if bytes.Equal(next, []byte(">=")) {
			ts.position += 2
			return tok{tokenGte, nil}, nil
		}
		ts.position++
		return tok{tokenGt, nil}, nil
	case '!':
		next := ts.peek(2)
		if bytes.Equal(next, []byte("!=")) {
			ts.position += 2
			return tok{tokenNeq, nil}, nil
		}
		ts.position++ // we need to advance the position, so that we don't get stuck
		return tok{}, errUnknownToken
	case '<':
		next := ts.peek(2)
		if bytes.Equal(next, []byte("<=")) {
			ts.position += 2
			return tok{tokenLte, nil}, nil
		}
		if bytes.Equal(next, []byte("<>")) {
			ts.position += 2
			return tok{tokenNeq, nil}, nil
		}
		ts.position++
		return tok{tokenLt, nil}, nil
	case '.', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		// TODO: well, test this thoroughly (especially the sad paths)
		// TODO: refactor into a method, that returns (tok, error), just like consumeStringLiteral
		//       this could also advance the position
		floatChars := []byte("0123456789e.-") // the minus here is not a unary minus, but for exponents - e.g. 2e-12
		intChars := []byte("0123456789")

		floatCandidate := sliceUntil(ts.code[ts.position:], floatChars)
		intCandidate := sliceUntil(ts.code[ts.position:], intChars)
		if len(intCandidate) == len(floatCandidate) {
			ts.position += len(intCandidate)
			if _, err := strconv.ParseInt(string(intCandidate), 10, 64); err != nil {
				return tok{}, errInvalidInteger
			}
			return tok{tokenLiteralInt, intCandidate}, nil
		}
		ts.position += len(floatCandidate)
		if _, err := strconv.ParseFloat(string(floatCandidate), 64); err != nil {
			// TODO: we're getting false negatives for 2*(1-d), where it tokenises the 1- as a float
			// also 1-3 gets tokenised as a single unit instead of three
			// solution? allow for - only at the beginning and after an e?
			return tok{}, errInvalidFloat
		}
		return tok{tokenLiteralFloat, floatCandidate}, nil
	case '\'': // string literal
		return ts.consumeStringLiteral()
	default:
		return ts.consumeIdentifier()
	}
}

// OPTIM: use a function with inequalities instead of this linear scan
func (ts *tokenScanner) consumeIdentifier() (tok, error) {
	ttoken := tok{ttype: tokenIdentifier}
	next := ts.peek(1)
	if bytes.Equal(next, []byte("\"")) {
		ttoken.ttype = tokenIdentifierQuoted
		ts.position++
	}

	// TODO: make sure we restrict columns to be this format
	identChars := []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_0123456789")
	ttoken.value = sliceUntil(ts.code[ts.position:], identChars)
	ts.position += len(ttoken.value)
	if len(ttoken.value) == 0 {
		// TODO: this is not quite the right error (it would be if we were quoted)
		// this actually means there's nothing identifier-like here
		ts.position++
		return tok{}, errInvalidIdentifier
	}
	if ttoken.ttype == tokenIdentifierQuoted {
		next := ts.peek(1)
		if !bytes.Equal(next, []byte("\"")) {
			return tok{}, errInvalidIdentifier
		}
		ts.position++
	}

	return ttoken, nil
}

const apostrophe = '\''

func (ts *tokenScanner) consumeStringLiteral() (tok, error) {
	token := tok{tokenLiteralString, nil}
	token.value = make([]byte, 0)
	for {
		idx := bytes.IndexByte(ts.code[ts.position+1:], apostrophe)
		if idx == -1 {
			ts.position++
			return tok{}, errInvalidString
		}
		chunk := ts.code[ts.position+1 : ts.position+idx+1]
		if bytes.IndexByte(chunk, '\n') > -1 {
			ts.position++
			return tok{}, fmt.Errorf("a string literal cannot contain a newline: %w", errInvalidString)
		}
		token.value = append(token.value, chunk...)
		ts.position += idx + 1
		next := ts.peek(2)
		// apostrophes can be in string literals, but they need to be escaped by another apostrophe
		if bytes.Equal(next, []byte("''")) {
			ts.position++
			token.value = append(token.value, apostrophe)
		} else {
			break
		}
	}

	ts.position++
	return token, nil
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
