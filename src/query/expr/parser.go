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
var errInvalidFunctionName = errors.New("invalid function name")
var errEmptyExpression = errors.New("cannot parse an expression from an empty string")
var errInvalidTuple = errors.New("invalid tuple expression")

const (
	_ int = iota
	LOWEST
	BOOL_AND_OR // TODO(next): is it really that AND and OR have the same precedence?
	EQUALS      // ==, !=
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
	tokenIn:     EQUALS,
	tokenNot:    EQUALS,
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
	prefixParseFn func() Expression
	infixParseFn  func(Expression) Expression
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
	tokensRaw, err := tokeniseString(s)
	if err != nil {
		return nil, err
	}
	// TODO(next)/ARCH: consider skipping comments in peekToken? But what about trailing comments,
	// won't the mess with "unparsed content"?
	tokens := make([]token, 0, len(tokensRaw))
	for _, tok := range tokensRaw {
		if tok.ttype == tokenComment {
			continue
		}
		tokens = append(tokens, tok)
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
		tokenAdd:              p.parsePrefixExpression,
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
		tokenIn:     p.parseInfixExpression,
		tokenNot:    p.parseInfixExpression,
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
func (p *Parser) parseIdentifer() Expression {
	val := p.curToken().value
	val = bytes.ToLower(val) // unquoted identifiers are case insensitive, so we can lowercase them
	return &Identifier{name: string(val)}
}
func (p *Parser) parseIdentiferQuoted() Expression {
	val := p.curToken().value
	return NewIdentifier(string(val))
}
func (p *Parser) parseLiteralInteger() Expression {
	// we don't need to do strconv validation - the tokeniser has done so already
	val, _ := strconv.ParseInt(string(p.curToken().value), 10, 64)
	return &Integer{value: val}
}
func (p *Parser) parseLiteralFloat() Expression {
	val, _ := strconv.ParseFloat(string(p.curToken().value), 64)
	return &Float{value: val}
}
func (p *Parser) parseLiteralString() Expression {
	return &String{value: string(p.curToken().value)}
}
func (p *Parser) parseLiteralNULL() Expression {
	return &Null{}
}
func (p *Parser) parseLiteralBool() Expression {
	// OPTIM: use a switch on p.curToken().ttype instead?
	val, _ := strconv.ParseBool(string(p.curToken().String()))

	return &Bool{value: val}
}

func (p *Parser) parseParentheses() Expression {
	p.position++
	expr := p.parseExpression(LOWEST)

	peek := p.peekToken()
	if peek.ttype != tokenRparen {
		p.errors = append(p.errors, errNoClosingBracket)
		return nil
	}
	p.position++

	return &Parentheses{inner: expr}
}
func (p *Parser) parsePrefixExpression() Expression {
	expr := &Prefix{operator: p.curToken().ttype}

	p.position++

	expr.right = p.parseExpression(PREFIX)

	return expr
}

func (p *Parser) parseCallExpression(left Expression) Expression {
	id, ok := left.(*Identifier)
	if !ok || id.quoted {
		p.errors = append(p.errors, fmt.Errorf("%w: %v", errInvalidFunctionName, left.String()))
		return nil
	}
	funName := id.name
	expr, err := NewFunction(funName)
	if err != nil {
		p.errors = append(p.errors, fmt.Errorf("error initialising function %v", funName))
		return nil
	}

	if p.peekToken().ttype == tokenRparen {
		p.position++
		return expr
	}
	p.position++
	expr.args = append(expr.args, p.parseExpression(LOWEST))

	for p.peekToken().ttype == tokenComma {
		p.position += 2
		expr.args = append(expr.args, p.parseExpression(LOWEST))
	}

	if p.peekToken().ttype != tokenRparen {
		p.errors = append(p.errors, errNoClosingBracket)
		return nil
	}
	p.position++

	return expr
}
func (p *Parser) parseInfixExpression(left Expression) Expression {
	curToken := p.curToken()
	expr := &Infix{operator: curToken.ttype, left: left}
	precedence := p.curPrecedence()
	p.position++

	// NOT is another exception ¯\_(ツ)_/¯
	// and a weird one, because it turns an infix operation to a prefix one (`foo NOT IN bar` -> `NOT(foo IN bar)`)
	if expr.operator == tokenNot || expr.operator == tokenIn {
		// will be used to extend LIKE and ILIKE to `NOT LIKE/ILIKE`
		if expr.operator == tokenNot && p.curToken().ttype != tokenIn {
			p.errors = append(p.errors, errors.New("infix operator NOT expects IN to follow"))
			return nil
		}
		neg := false
		if expr.operator == tokenNot {
			neg = true
			expr.operator = p.curToken().ttype // expr.operator is now e.g. `IN`, we'll wrap this infix be a prefix NOT
			p.position++
		}

		// TODO(next): this should be `p.parseTuple` for tokenIn and `p.parseStringLiteral` for tokenLike/tokenIlike
		expr.right = p.parseTuple(precedence)

		if neg {
			return &Prefix{operator: tokenNot, right: expr}
		}
		return expr
	}

	expr.right = p.parseExpression(precedence)
	return expr
}

// ARCH/TODO: I guess I don't need `precedence` here?
func (p *Parser) parseTuple(precedence int) Expression {
	if p.curToken().ttype != tokenLparen {
		p.errors = append(p.errors, fmt.Errorf("%w: IN clauses need to be followed by a parenthesised clause", errInvalidTuple))
		return nil
	}
	// foo in ()
	if p.peekToken().ttype == tokenRparen {
		p.errors = append(p.errors, fmt.Errorf("%w: cannot have an empty tuple", errInvalidTuple))
		return nil
	}
	p.position++
	inner, err := p.parseExpressions()
	if err != nil {
		p.errors = append(p.errors, err)
		return nil
	}
	if p.peekToken().ttype != tokenRparen {
		p.errors = append(p.errors, fmt.Errorf("%w: tuples need to end with a closing bracket", errInvalidTuple))
		return nil
	}
	p.position++
	return &Tuple{inner: inner}
}

func (p *Parser) parseExpression(precedence int) Expression {
	curToken := p.curToken()

	// `select * from foo` or `select *, foo from bar` etc.
	if curToken.ttype == tokenMul && (p.peekToken().ttype == tokenEOF || p.peekToken().ttype == tokenComma || p.peekToken().ttype == tokenFrom) {
		// ARCH: consider a custom type for this
		return &Identifier{name: "*"}
	}

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

func (p *Parser) parseOrdering() (Expression, error) {
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
	return &Ordering{Asc: asc, NullsFirst: nullsFirst}, nil
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
		if pt == tokenAs || pt == tokenIdentifier || pt == tokenIdentifierQuoted {
			p.position++
			if pt == tokenAs {
				p.position++
			}
			// relabeling is an exception, we use a different Expression for that
			target := p.parseExpression(LOWEST)
			label, ok := target.(*Identifier)
			if !ok {
				return nil, errors.New("when relabeling (AS), the right side value has to be an identifier")
			}
			expr = &Relabel{inner: expr, Label: label.name}
		}
		if pt == tokenAsc || pt == tokenDesc || pt == tokenNulls {
			oexp, err := p.parseOrdering()
			if err != nil {
				return nil, err
			}
			oexp.(*Ordering).inner = expr
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

func ParseStringExpr(s string) (Expression, error) {
	p, err := NewParser(s)
	if err != nil {
		return nil, err
	}
	if len(p.tokens) == 0 {
		return nil, errEmptyExpression
	}
	ret, err := p.parseExpressions()
	if err != nil {
		return nil, err
	}
	if len(ret) != 1 {
		return nil, fmt.Errorf("expected a single expression, got %v instead", len(ret))
	}

	if p.position != len(p.tokens)-1 {
		p.errors = append(p.errors, fmt.Errorf("%w: %v", errUnparsedBit, p.tokens[p.position:]))
	}

	// ARCH: abstract this into p.Err()? Will be useful if we do additional parsing (multiple expressions, select queries etc.)
	if len(p.errors) > 0 {
		return nil, fmt.Errorf("encountered %v errors, first one being: %w", len(p.errors), p.errors[0])
	}

	return ret[0], nil
}

func ParseStringExprs(s string) (ExpressionList, error) {
	p, err := NewParser(s)
	if err != nil {
		return nil, err
	}

	if len(p.tokens) == 0 {
		return nil, errEmptyExpression
	}

	exprs, err := p.parseExpressions()
	if err != nil {
		return nil, err
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
		// we expect FROM ... unless the query is without a dataset (e.g. `SELECT 1`)
		// ARCH: consider allowing for limits/having, I think they are supported elsewhere
		// TODO(next): "select 1 limit 100" NOT supported
		if p.peekToken().ttype == tokenEOF {
			return q, nil
		}
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

	return q, nil
}
