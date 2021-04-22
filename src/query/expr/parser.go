package expr

import (
	"bytes"
	"log"
)

// thank you, Thorsten
// TODO(PR): retype?
const (
	_ int = iota
	LOWEST
	EQUALS      // ==
	LESSGREATER // > or <
	SUM         // +
	PRODUCT     // *
	PREFIX      // -X or !X
	CALL        // myFunction(X)
)

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
		tokenIdentifier:       p.parseIdentifer,
		tokenIdentifierQuoted: p.parseIdentiferQuoted,
		tokenLiteralInt:       p.parseLiteralInteger,
		tokenLiteralFloat:     p.parseLiteralFloat,
		tokenSub:              p.parsePrefixExpression,
		tokenNot:              p.parsePrefixExpression,
	}

	return p, nil
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
func (p *Parser) parsePrefixExpression() *Expression {
	token := p.tokens[p.position]
	expr := &Expression{
		etype: exprPrefixOperator,
		// TODO: should we use token.value instead? We don't set it now...
		// also, this will make it quite clunky to match on
		value: token.String(),
	}

	p.position++

	right := p.parseExpression(PREFIX)
	expr.children = append(expr.children, right)

	return expr
}

func (p *Parser) parseExpression(precedence int) *Expression {
	prefix := p.prefixParseFns[p.tokens[p.position].ttype]
	if prefix == nil {
		// TODO(PR): proper error reporting? (like `p.errors` in the book)
		log.Fatalf("tried %v", p.tokens[p.position]) // TODO(PR): remove, just for debugging now
		return nil
	}

	return prefix()
}

func ParseStringExpr(s string) (*Expression, error) {
	p, err := NewParser(s)
	if err != nil {
		return nil, err
	}
	// TODO(PR)
	ret := p.parseExpression(LOWEST)

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
