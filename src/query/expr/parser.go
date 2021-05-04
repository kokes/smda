package expr

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"

	"github.com/kokes/smda/src/database"
)

var errUnparsedBit = errors.New("parsing incomplete")
var errNoClosingBracket = errors.New("no closing bracket after an opening one")
var errUnsupportedPrefixToken = errors.New("unsupported prefix token")
var errSQLOnlySelects = errors.New("only SELECT queries supported")

const (
	_ int = iota
	LOWEST
	BOOL_AND_OR // TODO(next): is it really that AND and OR have the same precedence?
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

var infixMapping = map[tokenType]exprType{
	tokenAnd: exprAnd,
	tokenOr:  exprOr,
	tokenAdd: exprAddition,
	tokenSub: exprSubtraction,
	tokenMul: exprMultiplication,
	tokenQuo: exprDivision,
	tokenEq:  exprEquality,
	tokenIs:  exprEquality,
	tokenNeq: exprNequality,
	tokenGt:  exprGreaterThan,
	tokenGte: exprGreaterThanEqual,
	tokenLt:  exprLessThan,
	tokenLte: exprLessThanEqual,
}

type (
	prefixParseFn func() *Expression
	infixParseFn  func(*Expression) *Expression
)

type Parser struct {
	tokens   tokenList
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

func (p *Parser) curToken() token {
	if p.position >= len(p.tokens) {
		return token{ttype: tokenEOF}
	}
	return p.tokens[p.position]
}

func (p *Parser) peekToken() token {
	if p.position >= len(p.tokens)-1 {
		return token{ttype: tokenEOF}
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

// ARCH: maybe don't build these as method but as functions (taking in Parser) and have them globally in a slice,
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
	curToken := p.curToken()
	var etype exprType
	switch curToken.ttype {
	case tokenSub:
		etype = exprUnaryMinus
	case tokenNot:
		etype = exprNot
	default:
		p.errors = append(p.errors, fmt.Errorf("%w: %v", errUnsupportedPrefixToken, curToken.ttype))
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
	curToken := p.curToken()
	etype, ok := infixMapping[curToken.ttype]
	if !ok {
		p.errors = append(p.errors, fmt.Errorf("unsupported infix operator: %v", curToken.ttype))
		return nil
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

// parse expressions separated by commas
func (p *Parser) parseExpressions() (ExpressionList, error) {
	var ret ExpressionList
	for {
		expr := p.parseExpression(LOWEST)
		// ARCH: abstract this into p.Err()? Will be useful if we do additional parsing (multiple expressions, select queries etc.)
		if len(p.errors) > 0 {
			return nil, fmt.Errorf("encountered %v errors, first one being: %w", len(p.errors), p.errors[0])
		}
		ret = append(ret, expr)

		ntype := p.peekToken().ttype
		switch ntype {
		case tokenComma:
			p.position += 2
		default:
			return ret, nil
		}
	}
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

func ParseStringExprs(s string) (ExpressionList, error) {
	// var ret []*Expression
	p, err := NewParser(s)
	if err != nil {
		return nil, err
	}

	exprs, err := p.parseExpressions()
	if err != nil {
		return nil, err
	}

	for _, expr := range exprs {
		if err := expr.InitFunctionCalls(); err != nil {
			return nil, err
		}
	}
	if p.position != len(p.tokens)-1 {
		p.errors = append(p.errors, fmt.Errorf("%w: %v", errUnparsedBit, p.tokens[p.position:]))
	}

	return exprs, nil
}

// type Query struct {
// 	Select    ExpressionList `json:"select,omitempty"`
// 	Dataset   database.UID   `json:"dataset"`
// 	Filter    *Expression    `json:"filter,omitempty"`
// 	Aggregate ExpressionList `json:"aggregate,omitempty"`
// 	Limit     *int           `json:"limit,omitempty"`
// }
// TODO(PR): finish this
func ParseQuerySQL(s string) (Query, error) {
	var q Query
	p, err := NewParser(s)
	if err != nil {
		return q, err
	}
	if p.curToken().ttype != tokenSelect {
		return q, errSQLOnlySelects
	}
	p.position++

	q.Select, err = p.parseExpressions()
	if err != nil {
		return q, err
	}

	// ARCH: we didn't increment position and used peekToken... elsewhere we use walk+curToken
	if p.peekToken().ttype != tokenFrom {
		// this will be for queries without a FROM clause, e.g. SELECT 1`
		panic("TODO(PR): at least serve a proper error here")
	}

	p.position += 2

	// TODO(next): sanitise dataset names by default + put guards in place do not allow anything non-ascii etc.

	// ARCH: allow for quoted identifiers? will depend on our rules on dataset names
	if p.curToken().ttype != tokenIdentifier {
		return q, fmt.Errorf("expecting dataset name, got %v", p.curToken())
	}
	dsn := p.curToken().value
	if len(dsn) == 0 || dsn[0] != 'v' {
		return q, fmt.Errorf("invalid dataset version, got %s", dsn)
	}
	q.Dataset, err = database.UIDFromHex(dsn[1:])
	if err != nil {
		return q, err
	}

	p.position++

	if p.curToken().ttype == tokenWhere {
		p.position++
		clause := p.parseExpression(LOWEST)
		// TODO(next): replace all p.errors with p.Err()
		if len(p.errors) > 0 {
			panic("TODO(PR)")
		}
		q.Filter = clause
		p.position++
	}
	if p.curToken().ttype == tokenGroup {
		p.position++
		if p.curToken().ttype != tokenBy {
			panic("TODO(PR)")
		}
		p.position++
		q.Aggregate, err = p.parseExpressions()
		if err != nil {
			return q, err
		}
		p.position++
	}

	if p.curToken().ttype == tokenLimit {
		p.position++
		if p.curToken().ttype != tokenLiteralInt {
			panic("TODO(PR): cannot limit by..." + p.curToken().String())
		}
		limit, err := strconv.Atoi(string(p.curToken().value))
		if err != nil {
			return q, err
		}
		q.Limit = &limit
	}

	// ARCH: using '<' to avoid issues with walking past the end (when using p.position++ instead of peekToken)
	if p.position < len(p.tokens)-1 {
		panic("TODO(PR): UNPARSED BIT: " + p.tokens.String())
	}

	// TODO/ARCH: this is repeated in multiple places, make it implicit?
	// also do this for all the Query fields that need this
	// for _, expr := range q.Select {
	// 	if err := expr.InitFunctionCalls(); err != nil {
	// 		return q, err
	// 	}
	// }

	return q, nil
}
