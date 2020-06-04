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
	"go/ast"
	"go/parser"
	"go/token"
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

func NewTokenScannerFromString(s string) *tokenScanner {
	return &tokenScanner{
		code: []byte(s),
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
func (ts *tokenScanner) Scan() (tok, error) {
	if ts.position >= len(ts.code) {
		return tok{tokenEOF, nil}, nil
	}
	char := ts.code[ts.position]
	switch char {
	// TODO: what about other utf whitespace? if we choose not to consider it as whitespace, test for it
	case ' ', '\t', '\n':
		ts.position++
		return ts.Scan()
	case ',':
		ts.position++
		return tok{tokenComma, nil}, nil
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
			return tok{}, errInvalidFloat
		}
		return tok{tokenLiteralFloat, floatCandidate}, nil
	case '\'': // string literal
		return ts.consumeStringLiteral()
	case '"': // quoted identifier
		// TODO: move all this logic to consumeIdentifier? (peek first, see if it starts with a quote etc.)
		ts.position++
		ident, err := ts.consumeIdentifier()
		if err != nil {
			return tok{}, err
		}
		next := ts.peek(1)
		ts.position++ // this is for the endquote
		if !bytes.Equal(next, []byte("\"")) {
			return tok{}, errInvalidIdentifier
		}
		ident.ttype = tokenIdentifierQuoted
		return ident, nil
	default:
		return ts.consumeIdentifier()
	}
}

func (ts *tokenScanner) consumeIdentifier() (tok, error) {
	// OPTIM: use a function with inequalities instead of this linear scan
	// TODO: make sure we restrict columns to be this format
	identChars := []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_0123456789")
	identCandidate := sliceUntil(ts.code[ts.position:], identChars)
	ts.position += len(identCandidate)
	if len(identCandidate) == 0 {
		// TODO: this is not quite the right error (it would be if we were quoted)
		// this actually means there's nothing identifier-like here
		ts.position++
		return tok{}, errInvalidIdentifier
	}
	return tok{tokenIdentifier, identCandidate}, nil
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
			return tok{}, errInvalidString // TODO: add context: cannot have a newline there
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

// TODO: pkgsplit - this would be a good place to split this into a tokeniser and a parser

type Projection interface {
	// isValid(tableSchema) - does it make sense to have this projection like this?
	//   - tableSchema = []columnSchema
	//   - checks that type are okay and everything
	// ReturnType dtype - though we'll have to pass in a schema
	// ColumnsUsed []string
	// isSimpleton (or something along those lines) - if this projection is just a column or a literal?
	//  - we might need a new typedColumn - columnLit{string,int,float,bool}?
}

// just an implementation of Projection - we might merge the two eventually
type Expression struct {
	// children []*Expression
	// value []byte/string
}

// limitations:
// - cannot use this for full query parsing, just expressions
// - cannot do count(*) and other syntactically problematic expressions (also ::)
// - limited use of = - we might use '==' for all equality for now and later switch to SQL's '='
//   - or we might silently promote any '=' to '==' (but only outside of strings...)
// - we cannot use escaped apostrophes in string literals (because Go can't parse that) - unless we sanitise that during tokenisation
// normal process: 1) tokenise, 2) build an ast, // 3) (optional) optimise the ast
// our process: 1) tokenise, 2) edit some of these tokens, 3) stringify and build an ast using a 3rd party, 4) optimise
// this is due to the fact that we don't have our own parser, we're using go's go/parser from the standard
// library - but we're leveraging our own tokeniser, because we need to "fix" some tokens before passing them
// to go/parser, because that parser is used for code parsing, not SQL expressions parsing
func ParseExpr(s string) (Projection, error) {
	// toks, err := tokeniseString(s) // helper function, TBA
	// toks = compatToks(toks)
	// s2 := stringify(toks) // strings.Builder etc. - will need a stringer for type tok
	tr, err := parser.ParseExpr(s)

	// we are fine with illegal rune literals - because we need e.g. 'ahoy' as literal strings
	if err != nil && !strings.HasSuffix(err.Error(), "illegal rune literal") {
		return nil, err
	}

	// switch tree.(type) - if the base is ast.BasicLit or ast.Ident, we can exit early

	fs := token.NewFileSet()
	ast.Print(fs, tr)

	return nil, nil
}

// func main() {
// 	// tree, err := ParseExpr("123*bak + nullif(\"foo\", 'abc')")
// 	tree, err := ParseExpr("(bak - 4) == (bar+3)")
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	_ = tree
// }
