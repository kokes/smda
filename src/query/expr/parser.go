package expr

import (
	"bytes"
	"errors"
	"fmt"
)

var errUnparsedBit = errors.New("parsing incomplete")
var errNoClosingBracket = errors.New("no closing bracket after an opening one")
var errUnsupportedPrefixToken = errors.New("unsupported prefix token")

// thank you, Thorsten
// TODO(PR): retype?
const (
	_ int = iota
	LOWEST
	BOOL_AND_OR // TODO(PR): is it really that AND and OR have the same precedence?
	EQUALS      // ==, !=
	// TODO(next): IN clause?
	LESSGREATER // >, <, <=, >=
	ADDITION    // +
	PRODUCT     // *
	PREFIX      // -X or NOT X
	CALL        // myFunction(X)
)

var precedences = map[tokenType]int{
	tokenAnd:    BOOL_AND_OR,
	tokenOr:     BOOL_AND_OR,
	tokenEq:     EQUALS,
	tokenIs:     EQUALS,
	tokenNeq:    EQUALS,
	tokenLt:     LESSGREATER,
	tokenGt:     LESSGREATER,
	tokenLte:    LESSGREATER,
	tokenGte:    LESSGREATER,
	tokenAdd:    ADDITION,
	tokenSub:    ADDITION,
	tokenQuo:    PRODUCT,
	tokenMul:    PRODUCT,
	tokenLparen: CALL,
}

type (
	prefixParseFn func() *Expression
	infixParseFn  func(*Expression) *Expression
)

type Parser struct {
	tokens   tokList
	position int
	errors   []error

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
		tokenIs:     p.parseInfixExpression,
		tokenNeq:    p.parseInfixExpression,
		tokenLt:     p.parseInfixExpression,
		tokenGt:     p.parseInfixExpression,
		tokenLte:    p.parseInfixExpression,
		tokenGte:    p.parseInfixExpression,
		tokenLparen: p.parseCallExpression,
	}

	return p, nil
}

func (p *Parser) curToken() tok {
	if p.position >= len(p.tokens) {
		return tok{}
	}
	return p.tokens[p.position]
}

func (p *Parser) peekToken() tok {
	if p.position >= len(p.tokens)-1 {
		return tok{}
	}
	return p.tokens[p.position+1]
}

func (p *Parser) curPrecedence() int {
	return precedences[p.curToken().ttype]
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
	val := p.curToken().value
	val = bytes.ToLower(val) // unquoted identifiers are case insensitive, so we can lowercase them
	return &Expression{etype: exprIdentifier, value: string(val)}
}
func (p *Parser) parseIdentiferQuoted() *Expression {
	val := p.curToken().value
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
	val := string(p.curToken().value)
	// we don't need to do strconv validation - the tokeniser has done so already
	return &Expression{etype: exprLiteralInt, value: val}
}
func (p *Parser) parseLiteralFloat() *Expression {
	val := string(p.curToken().value)
	return &Expression{etype: exprLiteralFloat, value: val}
}
func (p *Parser) parseLiteralString() *Expression {
	val := p.curToken().value
	return &Expression{etype: exprLiteralString, value: string(val)}
}
func (p *Parser) parseLiteralNULL() *Expression {
	return &Expression{etype: exprLiteralNull}
}
func (p *Parser) parseLiteralBool() *Expression {
	val := p.curToken()
	return &Expression{etype: exprLiteralBool, value: val.String()}
}

func (p *Parser) parseParentheses() *Expression {
	p.position++
	expr := p.parseExpression(LOWEST)

	peek := p.peekToken()
	if peek.ttype != tokenRparen {
		p.errors = append(p.errors, errNoClosingBracket)
		return nil
	}
	p.position++
	expr.parens = true

	return expr
}
func (p *Parser) parsePrefixExpression() *Expression {
	token := p.curToken()
	var etype exprType
	switch token.ttype {
	case tokenSub:
		etype = exprUnaryMinus
	case tokenNot:
		etype = exprNot
	default:
		p.errors = append(p.errors, fmt.Errorf("%w: %v", errUnsupportedPrefixToken, token.ttype))
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
		p.errors = append(p.errors, errNoClosingBracket)
		return nil
	}
	p.position++

	return expr
}
func (p *Parser) parseInfixExpression(left *Expression) *Expression {
	var etype exprType
	curToken := p.curToken()
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
	case tokenIs:
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
	precedence := p.curPrecedence()
	p.position++
	right := p.parseExpression(precedence)

	expr.children = []*Expression{left, right}

	return expr
}

func (p *Parser) parseExpression(precedence int) *Expression {
	curToken := p.curToken()
	prefix := p.prefixParseFns[curToken.ttype]
	if prefix == nil {
		p.errors = append(p.errors, fmt.Errorf("%w: %v", errUnsupportedPrefixToken, curToken))
		return nil
	}

	left := prefix()

	for precedence < p.peekPrecedence() {
		nextToken := p.peekToken()
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
		p.errors = append(p.errors, fmt.Errorf("%w: %v", errUnparsedBit, p.tokens[p.position:]))
	}

	// ARCH: abstract this into p.Err()? Will be useful if we do additional parsing (multiple expressions, select queries etc.)
	if len(p.errors) > 0 {
		return nil, fmt.Errorf("encountered %v errors, first one being: %w", len(p.errors), p.errors[0])
	}

	if err := ret.InitFunctionCalls(); err != nil {
		return nil, err
	}

	return ret, nil
}
