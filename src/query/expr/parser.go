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
var errInvalidQuery = errors.New("invalid SQL query")

const (
	_ int = iota
	LOWEST
	RELABEL     // foo AS bar
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
	tokenAs:     RELABEL,
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
	tokenAs:  exprRelabel,
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
		tokenAs:     p.parseInfixExpression,
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

func (p *Parser) parseOrdering() (*Expression, error) {
	asc := true
	nullsFirst := false
	if (p.peekToken().ttype == tokenAsc) || (p.peekToken().ttype == tokenDesc) {
		asc = p.peekToken().ttype == tokenAsc
		nullsFirst = !asc
		p.position++
	}
	if p.peekToken().ttype == tokenNulls {
		p.position++
		if !(p.peekToken().ttype == tokenFirst || p.peekToken().ttype == tokenLast) {
			return nil, fmt.Errorf("%w: expecting NULLS to be followed by FIRST or LAST", errInvalidQuery)
		}
		nullsFirst = p.peekToken().ttype == tokenFirst
		p.position++
	}
	var method string
	// ARCH: eeeeek
	if asc {
		if nullsFirst {
			method = SortAscNullsFirst
		} else {
			method = SortAscNullsLast
		}
	} else {
		if nullsFirst {
			method = SortDescNullsFirst
		} else {
			method = SortDescNullsLast
		}
	}
	return &Expression{etype: exprSort, value: method}, nil
}

func (p *Parser) Err() error {
	if len(p.errors) == 0 {
		return nil
	}
	return fmt.Errorf("encountered %v errors, first one being: %w", len(p.errors), p.errors[0])
}

// parse expressions separated by commas
func (p *Parser) parseExpressions() (ExpressionList, error) {
	var ret ExpressionList
	for {
		expr := p.parseExpression(LOWEST)
		pt := p.peekToken().ttype
		if pt == tokenAsc || pt == tokenDesc || pt == tokenNulls {
			oexp, err := p.parseOrdering()
			if err != nil {
				return nil, err
			}
			oexp.children = []*Expression{expr}
			expr, oexp = oexp, expr
		}

		if err := p.Err(); err != nil {
			return nil, err
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
	// ARCH/TODO(next): perhaps return noExpression or something?
	// if len(p.tokens) == 0 {
	// 	return nil, nil
	// }
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
	p, err := NewParser(s)
	if err != nil {
		return nil, err
	}

	// ARCH/TODO(next): perhaps return noExpression or something?
	if len(p.tokens) == 0 {
		return nil, nil
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
		return q, fmt.Errorf("%w: expecting FROM after expression list, got %v instead", errInvalidQuery, p.peekToken())
	}

	p.position += 2

	// TODO(next): sanitise dataset names by default + put guards in place do not allow anything non-ascii etc.

	// ARCH: allow for quoted identifiers? will depend on our rules on dataset names
	if p.curToken().ttype != tokenIdentifier {
		return q, fmt.Errorf("expecting dataset name, got %v", p.curToken())
	}
	datasetID := database.DatasetIdentifier{
		Name:    string(p.curToken().value),
		Version: database.UID{},
		Latest:  true,
	}
	if p.peekToken().ttype == tokenAt {
		p.position += 2
		if p.curToken().ttype != tokenIdentifier {
			return q, fmt.Errorf("%w: expecting dataset version after @", errInvalidQuery)
		}
		dsn := p.curToken().value
		if len(dsn) == 0 || dsn[0] != 'v' {
			return q, fmt.Errorf("invalid dataset version, got %s", dsn)
		}
		datasetID.Version, err = database.UIDFromHex(dsn[1:])
		if err != nil {
			return q, err
		}
		datasetID.Latest = false
	}
	q.Dataset = &datasetID

	p.position++

	if p.curToken().ttype == tokenWhere {
		p.position++
		clause := p.parseExpression(LOWEST)
		if err := p.Err(); err != nil {
			return q, err
		}
		q.Filter = clause
		p.position++
	}
	if p.curToken().ttype == tokenGroup {
		p.position++
		if p.curToken().ttype != tokenBy {
			return q, fmt.Errorf("%w: expecting GROUP to be followed by BY", errInvalidQuery)
		}
		p.position++
		q.Aggregate, err = p.parseExpressions()
		if err != nil {
			return q, err
		}
		p.position++
	}

	if p.curToken().ttype == tokenOrder {
		p.position++
		if p.curToken().ttype != tokenBy {
			return q, fmt.Errorf("%w: expecting ORDER to be followed by BY", errInvalidQuery)
		}
		p.position++
		q.Order, err = p.parseExpressions()
		if err != nil {
			return q, err
		}
		p.position++
	}

	if p.curToken().ttype == tokenLimit {
		p.position++
		if p.curToken().ttype != tokenLiteralInt {
			return q, fmt.Errorf("%w: can only LIMIT by integers", errInvalidQuery)
		}
		limit, err := strconv.Atoi(string(p.curToken().value))
		if err != nil {
			return q, err
		}
		q.Limit = &limit
	}

	// ARCH: using '<' to avoid issues with walking past the end (when using p.position++ instead of peekToken)
	if p.position < len(p.tokens)-1 {
		return q, fmt.Errorf("%w: incomplete parsing of supplied query", errInvalidQuery)
	}

	// TODO/ARCH: this is repeated in multiple places, make it implicit?
	// also do this for all the Query fields that need this
	// maybe have something like query.InitFunctionCalls and remove it from the parser altogether?
	if q.Select != nil {
		for _, expr := range q.Select {
			if err := expr.InitFunctionCalls(); err != nil {
				return q, err
			}
		}
	}
	if q.Aggregate != nil {
		for _, expr := range q.Aggregate {
			if err := expr.InitFunctionCalls(); err != nil {
				return q, err
			}
		}
	}
	if q.Filter != nil {
		if err := q.Filter.InitFunctionCalls(); err != nil {
			return q, err
		}
	}

	return q, nil
}
