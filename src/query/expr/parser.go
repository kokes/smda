package expr

// thank you, Thorsten
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

type Parser struct {
	tokens   tokList
	position int
	// infix/postfix functions
}

func NewParser(s string) (*Parser, error) {
	// OPTIM: walk it here without materialising it... but it shouldn't really matter for our use cases
	tokens, err := tokeniseString(s)
	if err != nil {
		return nil, err
	}
	return &Parser{
		tokens: tokens,
	}, nil
}

// limitations (fix this for the custom_parser - TODO(PR)):
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
// when building our own parser, consider:
// isPrecedence: get inspired: https://golang.org/src/go/token/token.go?s=4316:4348#L253
//  - then build an expression parser with precedence built in
func ParseStringExpr(s string) (*Expression, error) {
	p, err := NewParser(s)
	if err != nil {
		return nil, err
	}
	// TODO(PR)
	_ = p
	// func (p *Parser) parseExpressionStatement() *ast.ExpressionStatement {
	// 	stmt := &ast.ExpressionStatement{Token: p.curToken}

	// 	stmt.Expression = p.parseExpression(LOWEST)
	// 	if p.peekTokenIs(token.SEMICOLON) {
	// 		p.nextToken()
	// 	}
	// 	return stmt
	// }

	return &Expression{}, nil
}

// func (p *Parser) parseExpression(precedence int) ast.Expression {
// 	prefix := p.prefixParseFns[p.curToken.Type]
// 	if prefix == nil {
// 		return nil
// 	}
// 	leftExp := prefix()
// 	return leftExp
// }
