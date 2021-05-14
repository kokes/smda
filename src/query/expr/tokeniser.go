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

type token struct {
	ttype tokenType
	value []byte
}
type tokenList []token

const (
	tokenInvalid tokenType = iota
	tokenIdentifier
	tokenIdentifierQuoted
	tokenComment
	// keywords:
	tokenSelect
	tokenFrom
	tokenAt
	tokenWhere
	// tokenJoin
	// tokenOn
	// tokenLeft
	// tokenRight
	// tokenInner
	// tokenOuter
	// tokenFull
	tokenGroup
	tokenBy
	tokenLimit
	tokenOrder
	tokenAsc
	tokenDesc
	tokenNulls
	tokenFirst
	tokenLast
	// non-select keywords:
	tokenAnd
	tokenOr
	tokenAs
	tokenTrue
	tokenFalse
	tokenNull
	tokenIn
	tokenLike
	tokenIlike
	tokenIs
	tokenNot
	tokenCase
	tokenWhen
	tokenEnd
	// keywords end
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

var keywords = map[string]tokenType{
	"and":    tokenAnd,
	"or":     tokenOr,
	"as":     tokenAs,
	"true":   tokenTrue,
	"false":  tokenFalse,
	"null":   tokenNull,
	"in":     tokenIn,
	"like":   tokenLike,
	"ilike":  tokenIlike,
	"is":     tokenIs,
	"not":    tokenNot,
	"case":   tokenCase,
	"when":   tokenWhen,
	"end":    tokenEnd,
	"select": tokenSelect,
	"from":   tokenFrom,
	"where":  tokenWhere,
	"group":  tokenGroup,
	"by":     tokenBy,
	"limit":  tokenLimit,
	"order":  tokenOrder,
	"asc":    tokenAsc,
	"desc":   tokenDesc,
	"nulls":  tokenNulls,
	"first":  tokenFirst,
	"last":   tokenLast,
}

// ARCH: it might be useful to just use .value in most cases here
func (tok token) String() string {
	switch tok.ttype {
	case tokenIdentifier:
		return string(tok.value)
	case tokenIdentifierQuoted:
		return fmt.Sprintf("\"%s\"", tok.value)
	case tokenComment:
		return fmt.Sprintf("-- %v\n", tok.value)
	case tokenAnd:
		return "AND"
	case tokenOr:
		return "OR"
	case tokenAs:
		return "AS"
	case tokenTrue:
		return "TRUE"
	case tokenFalse:
		return "FALSE"
	case tokenNull:
		return "NULL"
	case tokenIn:
		return "IN"
	case tokenLike:
		return "LIKE"
	case tokenIlike:
		return "ILIKE"
	case tokenIs:
		return "IS"
	case tokenNot:
		return "NOT"
	case tokenCase:
		return "CASE"
	case tokenWhen:
		return "WHEN"
	case tokenEnd:
		return "END"
	case tokenSelect:
		return "SELECT"
	case tokenFrom:
		return "FROM"
	case tokenAt:
		return "@"
	case tokenWhere:
		return "WHERE"
	case tokenGroup:
		return "GROUP"
	case tokenBy:
		return "BY"
	case tokenLimit:
		return "LIMIT"
	case tokenOrder:
		return "ORDER"
	case tokenAsc:
		return "ASC"
	case tokenDesc:
		return "DESC"
	case tokenNulls:
		return "NULLS"
	case tokenFirst:
		return "FIRST"
	case tokenLast:
		return "LAST"
	case tokenAdd:
		return "+"
	case tokenSub:
		return "-"
	case tokenMul:
		return "*"
	case tokenQuo:
		return "/"
	case tokenEq:
		return "="
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
	case tokenInvalid:
		return "invalid_token"
	case tokenEOF:
		return "EOF"
	default:
		panic(fmt.Sprintf("unknown token type: %v", tok.ttype))
	}
}

func (tokens tokenList) String() string {
	var sb strings.Builder
	for j, tok := range tokens {
		sb.WriteString(tok.String())
		if j != len(tokens)-1 {
			sb.WriteByte(' ')
		}
	}
	return sb.String()
}

type tokenScanner struct {
	code     []byte
	position int
}

func newTokenScanner(s []byte) *tokenScanner {
	return &tokenScanner{
		code: s,
	}
}

func newTokenScannerFromString(s string) *tokenScanner {
	return newTokenScanner([]byte(s))
}

func tokeniseString(s string) (tokenList, error) {
	scanner := newTokenScannerFromString(s)
	var tokens []token
	for {
		tok, err := scanner.scan()
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

func (ts *tokenScanner) peekOne() byte {
	return ts.peek(1)[0]
}

func (ts *tokenScanner) scan() (token, error) {
	if ts.position >= len(ts.code) {
		return token{tokenEOF, nil}, nil
	}
	char := ts.code[ts.position]
	switch char {
	case ' ', '\t', '\n':
		ts.position++
		return ts.scan()
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
		return ts.consumeNumber()
	case '@':
		ts.position++
		return token{tokenAt, nil}, nil
	case '\'': // string literal
		return ts.consumeStringLiteral()
	default:
		// doesn't have to be an identifier, could be a keyword
		ident, err := ts.consumeIdentifier()
		if err != nil {
			return token{}, err
		}
		// quoted identifiers cannot be mistaken for keywords
		if ident.ttype == tokenIdentifier {
			identl := strings.ToLower(string(ident.value))
			if kw, ok := keywords[identl]; ok {
				return token{ttype: kw}, nil
			}
		}

		return ident, nil
	}
}

func (ts *tokenScanner) consumeNumber() (token, error) {
	var (
		seenDecPoint bool
		seenExp      bool
	)
	char := ts.code[ts.position]
	if char == '.' {
		seenDecPoint = true
	}
	val := []byte{char}
	ts.position++
	intChars := []byte("0123456789")
	ints := sliceUntil(ts.code[ts.position:], intChars)
	ts.position += len(ints)
	val = append(val, ints...)

scan:
	for {
		switch ts.peekOne() {
		case '.':
			if seenDecPoint {
				break scan
			}
			seenDecPoint = true
			val = append(val, '.')
			ts.position++
			ints = sliceUntil(ts.code[ts.position:], intChars)
			ts.position += len(ints)
			val = append(val, ints...)
		case 'e':
			if seenExp {
				break scan
			}
			seenExp = true
			val = append(val, 'e')
			ts.position++
			if ts.peekOne() == '-' {
				ts.position++
				val = append(val, '-')
			}
			ints = sliceUntil(ts.code[ts.position:], intChars)
			ts.position += len(ints)
			val = append(val, ints...)
			break scan
		default:
			break scan
		}
	}

	if seenDecPoint || seenExp {
		if _, err := strconv.ParseFloat(string(val), 64); err != nil {
			return token{}, errInvalidFloat
		}
		return token{tokenLiteralFloat, val}, nil
	}
	if _, err := strconv.ParseInt(string(val), 10, 64); err != nil {
		return token{}, errInvalidInteger
	}
	return token{tokenLiteralInt, val}, nil
}

// OPTIM: use a function with inequalities instead of this linear scan
func (ts *tokenScanner) consumeIdentifier() (token, error) {
	ttoken := token{ttype: tokenIdentifier}
	if ts.peekOne() == '"' {
		// TODO: quoted identifier should allow for more characters - basically anything
		// but a newline of a quote - unless preceded by a quote - should be fair game
		// So maybe split this out into consumeQuotedIdentifier?
		// ARCH: will we allow (escaped) quotes in names of identifiers? (Let's not...)
		ttoken.ttype = tokenIdentifierQuoted
		ts.position++
		quotepos := bytes.IndexByte(ts.code[ts.position:], '"')
		nlinepos := bytes.IndexByte(ts.code[ts.position:], '\n')
		if quotepos == -1 {
			return token{}, fmt.Errorf("%w: no matching quote in quoted identifier", errInvalidIdentifier)
		}
		if quotepos == 0 {
			return token{}, fmt.Errorf("%w: identifiers cannot be empty", errInvalidIdentifier)
		}
		if nlinepos > -1 && nlinepos < quotepos {
			return token{}, fmt.Errorf("%w: newline in quoted identifier", errInvalidIdentifier)
		}
		ttoken.value = ts.code[ts.position : ts.position+quotepos]
		ts.position += quotepos + 1
		return ttoken, nil
	}

	identChars := []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_0123456789")
	ttoken.value = sliceUntil(ts.code[ts.position:], identChars)
	ts.position += len(ttoken.value)
	if len(ttoken.value) == 0 {
		// TODO: this is not quite the right error (it would be if we were quoted)
		// this actually means there's nothing identifier-like here
		// a good way to check would be to see if the first char is one of those a-zA-Z_0-9 and if not, fallthrough to invalididentifier
		ts.position++
		return token{}, errInvalidIdentifier
	}

	return ttoken, nil
}

const apostrophe = '\''

func (ts *tokenScanner) consumeStringLiteral() (token, error) {
	ret := token{tokenLiteralString, nil}
	ret.value = make([]byte, 0)
	for {
		idx := bytes.IndexByte(ts.code[ts.position+1:], apostrophe)
		if idx == -1 {
			ts.position++
			return token{}, errInvalidString
		}
		chunk := ts.code[ts.position+1 : ts.position+idx+1]
		if bytes.IndexByte(chunk, '\n') > -1 {
			ts.position++
			return token{}, fmt.Errorf("a string literal cannot contain a newline: %w", errInvalidString)
		}
		ret.value = append(ret.value, chunk...)
		ts.position += idx + 1
		next := ts.peek(2)
		// apostrophes can be in string literals, but they need to be escaped by another apostrophe
		if bytes.Equal(next, []byte("''")) {
			ts.position++
			ret.value = append(ret.value, apostrophe)
		} else {
			break
		}
	}

	ts.position++
	return ret, nil
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
