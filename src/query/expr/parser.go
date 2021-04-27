package expr

import (
	"bytes"
	"fmt"
	"log"
)

// thank you, Thorsten
// TODO(PR): retype?
const (
	_ int = iota
	LOWEST
	BOOL_AND_OR // TODO(PR): is it really that AND and OR have the same precedence?
	EQUALS      // ==
	LESSGREATER // > or <
	SUM         // +, TODO(PR): rename to ADDITION?
	PRODUCT     // *
	PREFIX      // -X or !X
	CALL        // myFunction(X)
)

var precedences = map[tokenType]int{
	tokenAnd:    BOOL_AND_OR,
	tokenOr:     BOOL_AND_OR,
	tokenEq:     EQUALS,
	tokenNeq:    EQUALS,
	tokenLt:     LESSGREATER,
	tokenGt:     LESSGREATER,
	tokenLte:    LESSGREATER,
	tokenGte:    LESSGREATER,
	tokenAdd:    SUM,
	tokenSub:    SUM,
	tokenQuo:    PRODUCT,
	tokenMul:    PRODUCT,
	tokenLparen: CALL,
	// TODO(PR): tokenIs, tokenAnd, tokenOr (look up Go code and their precedence tables)
}

type (
	prefixParseFn func() *Expression
	infixParseFn  func(*Expression) *Expression
)

type Parser struct {
	tokens   tokList
	position int

	prefixParseFns map[tokenType]prefixParseFn
	infixParseFns  map[tokenType]infixParseFn
}

func NewParser(s string) (*Parser, error) {
	// OPTIM: walk it here without materialising it... but it shouldn't really matter for our use cases
	tokens, err := tokeniseString(s)
	if err != nil {
		return nil, err
	}
	p := &Parser{tokens: tokens}
	p.prefixParseFns = map[tokenType]prefixParseFn{
		tokenLparen:           p.parseParentheses,
		tokenIdentifier:       p.parseIdentifer,
		tokenIdentifierQuoted: p.parseIdentiferQuoted,
		tokenLiteralInt:       p.parseLiteralInteger,
		tokenLiteralFloat:     p.parseLiteralFloat,
		tokenLiteralString:    p.parseLiteralString,
		tokenTrue:             p.parseLiteralBool,
		tokenFalse:            p.parseLiteralBool,
		tokenNull:             p.parseLiteralNULL,
		tokenSub:              p.parsePrefixExpression,
		tokenNot:              p.parsePrefixExpression,
	}
	p.infixParseFns = map[tokenType]infixParseFn{
		tokenAnd:    p.parseInfixExpression,
		tokenOr:     p.parseInfixExpression,
		tokenAdd:    p.parseInfixExpression,
		tokenSub:    p.parseInfixExpression,
		tokenQuo:    p.parseInfixExpression,
		tokenMul:    p.parseInfixExpression,
		tokenEq:     p.parseInfixExpression,
		tokenNeq:    p.parseInfixExpression,
		tokenLt:     p.parseInfixExpression,
		tokenGt:     p.parseInfixExpression,
		tokenLte:    p.parseInfixExpression,
		tokenGte:    p.parseInfixExpression,
		tokenLparen: p.parseCallExpression,
		// TODO(PR): is
	}

	return p, nil
}

func (p *Parser) peekToken() tok {
	if p.position >= len(p.tokens)-1 {
		return tok{}
	}
	return p.tokens[p.position+1]
}

func (p *Parser) peekPrecedence() int {
	pt := p.peekToken()
	if pt.ttype == tokenInvalid {
		return LOWEST
	}
	return precedences[pt.ttype]
}

// TODO(PR): maybe don't build these as method but as functions (taking in Parser) and have them globally in a slice,
// not in a map for each parser
func (p *Parser) parseIdentifer() *Expression {
	val := p.tokens[p.position].value
	val = bytes.ToLower(val) // unquoted identifiers are case insensitive, so we can lowercase them
	return &Expression{etype: exprIdentifier, value: string(val)}
}
func (p *Parser) parseIdentiferQuoted() *Expression {
	val := p.tokens[p.position].value
	etype := exprIdentifier
	// only assign the Quoted variant if there's a need for it
	// TODO/ARCH: what about '-'? In general, what are our rules for quoting?
	for _, char := range val {
		if !((char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') || (char == '_')) {
			etype = exprIdentifierQuoted
			break
		}
	}

	return &Expression{etype: etype, value: string(val)}
}
func (p *Parser) parseLiteralInteger() *Expression {
	val := p.tokens[p.position].value
	// TODO(PR): validate using strconv
	return &Expression{etype: exprLiteralInt, value: string(val)}
}
func (p *Parser) parseLiteralFloat() *Expression {
	val := p.tokens[p.position].value
	// TODO(PR): validate using strconv
	return &Expression{etype: exprLiteralFloat, value: string(val)}
}
func (p *Parser) parseLiteralString() *Expression {
	val := p.tokens[p.position].value
	return &Expression{etype: exprLiteralString, value: string(val)}
}
func (p *Parser) parseLiteralNULL() *Expression {
	return &Expression{etype: exprLiteralNull}
}
func (p *Parser) parseLiteralBool() *Expression {
	val := p.tokens[p.position]
	return &Expression{etype: exprLiteralBool, value: val.String()}
}

func (p *Parser) parseParentheses() *Expression {
	p.position++
	expr := p.parseExpression(LOWEST)

	peek := p.peekToken()
	if peek.ttype != tokenRparen {
		// TODO(PR): error reporting (or upstream?)
		return nil
	}
	p.position++
	expr.parens = true

	return expr
}
func (p *Parser) parsePrefixExpression() *Expression {
	token := p.tokens[p.position]
	var etype exprType
	switch token.ttype {
	case tokenSub:
		etype = exprUnaryMinus
	case tokenNot:
		etype = exprNot
	default:
		// TODO(PR): error reporting
		return nil
	}
	expr := &Expression{
		etype: etype,
	}

	p.position++

	right := p.parseExpression(PREFIX)
	expr.children = append(expr.children, right)

	return expr
}

func (p *Parser) parseCallExpression(left *Expression) *Expression {
	funName := left.value
	expr := &Expression{etype: exprFunCall, value: funName}

	if p.peekToken().ttype == tokenRparen {
		p.position++
		return expr
	}
	p.position++
	expr.children = append(expr.children, p.parseExpression(LOWEST))

	for p.peekToken().ttype == tokenComma {
		p.position += 2
		expr.children = append(expr.children, p.parseExpression(LOWEST))
	}

	if p.peekToken().ttype != tokenRparen {
		// TODO(PR): error reporting
		return nil
	}
	p.position++

	return expr
}
func (p *Parser) parseInfixExpression(left *Expression) *Expression {
	var etype exprType
	curToken := p.tokens[p.position]
	// TODO(PR)/ARCH: this could be done in a map[tokenType]exprType?
	// or maybe, in the future, we could have an exprOperator? That would house all of these?
	switch curToken.ttype {
	case tokenAnd:
		etype = exprAnd
	case tokenOr:
		etype = exprOr
	case tokenAdd:
		etype = exprAddition
	case tokenSub:
		etype = exprSubtraction
	case tokenMul:
		etype = exprMultiplication
	case tokenQuo:
		etype = exprDivision
	case tokenEq:
		etype = exprEquality
	case tokenNeq:
		etype = exprNequality
	case tokenGt:
		etype = exprGreaterThan
	case tokenGte:
		etype = exprGreaterThanEqual
	case tokenLt:
		etype = exprLessThan
	case tokenLte:
		etype = exprLessThanEqual
	default:
		panic("TODO(PR)" + fmt.Sprintf("%v AND %v", left, curToken))
	}
	expr := &Expression{etype: etype}
	precedence := precedences[p.tokens[p.position].ttype] // TODO(PR): consider moving all this to curPrecedence?
	p.position++
	right := p.parseExpression(precedence)

	expr.children = []*Expression{left, right}

	return expr
}

func (p *Parser) parseExpression(precedence int) *Expression {
	prefix := p.prefixParseFns[p.tokens[p.position].ttype]
	if prefix == nil {
		// TODO(PR): proper error reporting? (like `p.errors` in the book)
		log.Fatalf("tried %v", p.tokens[p.position]) // TODO(PR): remove, just for debugging now
		return nil
	}

	left := prefix()

	for precedence < p.peekPrecedence() {
		nextToken := p.tokens[p.position+1]
		infix := p.infixParseFns[nextToken.ttype]
		if infix == nil {
			return left
		}
		p.position++
		left = infix(left)
	}
	return left
}

func ParseStringExpr(s string) (*Expression, error) {
	p, err := NewParser(s)
	if err != nil {
		return nil, err
	}
	ret := p.parseExpression(LOWEST)

	if p.position != len(p.tokens)-1 {
		// TODO(PR)/ARCH: standardise and wrap error
		return nil, fmt.Errorf("unparsed bit: %v", p.tokens[p.position:])
	}

	// TODO(PR): also if len(p.errors) > 0 ...

	if err := ret.InitFunctionCalls(); err != nil {
		return nil, err
	}

	return ret, nil
}

// TODO(PR): reflect these notes on ParseStringExpr in tests:
// limitations (fix this for the custom_parser - TODO(PR)):
// - cannot use this for full query parsing, just expressions
// - cannot do count(*) and other syntactically problematic expressions (also ::)
// - we cannot use escaped apostrophes in string literals (because Go can't parse that) - unless we sanitise that during tokenisation
// normal process: 1) tokenise, 2) build an ast, // 3) (optional) optimise the ast
// our process: 1) tokenise, 2) edit some of these tokens, 3) stringify and build an ast using a 3rd party, 4) optimise
// this is due to the fact that we don't have our own parser, we're using go's go/parser from the standard
// library - but we're leveraging our own tokeniser, because we need to "fix" some tokens before passing them
// to go/parser, because that parser is used for code parsing, not SQL expressions parsing
// when building our own parser, consider:
// isPrecedence: get inspired: https://golang.org/src/go/token/token.go?s=4316:4348#L253
//  - then build an expression parser with precedence built in
