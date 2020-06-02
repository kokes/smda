package smda

type token uint8

// TODO: stringer
const (
	tokenInvalid token = iota
	// tokenIdentifier
	// tokenComment
	tokenAdd
	tokenSub
	tokenMul
	tokenQuo
	tokenEq
	tokenNeq // !=, but what about <>?
	// tokenGt
	// tokenLt
	// tokenGte
	// tokenLte
	tokenLparen
	tokenRparen
	tokenComma
	// tokenLiteralInt
	// tokenLiteralFloat
	// tokenLiteralString
	// tokenLiteralBool
	// tokenAnd
	// tokenOr
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

func (ts *tokenScanner) Scan() (tok token, value []byte) {
	if ts.position >= len(ts.code) {
		return tokenEOF, nil
	}
	char := ts.code[ts.position]
	switch char {
	case '+':
		ts.position++
		return tokenAdd, nil
	case '-':
		// TODO: handle comments
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
	default:
		panic("unknown token")
	}
}

// func tokenise(input []byte) ([]token, error) {
// 	cPos := 0
// 	var tokens []token
// 	for cPos < len(input) {
// 		cChar := input[cPos]
// 		switch cChar {
// 		case '+':
// 			tokens = append(tokens, tokenAdd)

// 		}
// 		cPos++
// 	}
// 	return nil, nil
// }
