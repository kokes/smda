package expr

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strings"
)

type Projection interface {
	// isValid(tableSchema) - does it make sense to have this projection like this?
	//   - tableSchema = []columnSchema
	//   - checks that type are okay and everything
	// ReturnType dtype - though we'll have to pass in a schema
	// ColumnsUsed []string
	// isSimpleton (or something along those lines) - if this projection is just a column or a literal?
	//  - we might need a new typedColumn - columnLit{string,int,float,bool}?
}

type exprType uint8

// TODO: stringer
const (
	exprInvalid exprType = iota
	exprIdentifier
	exprAddition
	exprSubtraction
	exprMultiplication
	exprDivision
	exprEquality
	exprNequality
	exprLessThan
	exprLessThanEqual
	exprGreaterThan
	exprGreaterThanEqual
	exprLiteralInt
	exprLiteralFloat
	exprLiteralString
	// TODO: other literals, esp. bool
	exprFunCall
)

// just an implementation of Projection - we might merge the two eventually
type Expression struct {
	etype    exprType
	children []*Expression
	value    string
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
func ParseStringExpr(s string) (Projection, error) {
	tokens, err := TokeniseString(s)
	if err != nil {
		return nil, err
	}
	// we could have used ParseExpr directly, but we need to sanitise it first, because Go's parser
	// doesn't work well with SQL-like expressions
	// we won't need this as soon as we have a custom parser
	s2 := tokens.String()
	tr, err := parser.ParseExpr(s2)

	// we are fine with illegal rune literals - because we need e.g. 'ahoy' as literal strings
	if err != nil && !strings.HasSuffix(err.Error(), "illegal rune literal") {
		return nil, err
	}

	tree, err := convertAstExprToOwnExpr(tr)

	return tree, err
}

func convertAstExprToOwnExpr(expr ast.Expr) (*Expression, error) {
	switch expr.(type) {
	case *ast.Ident:
		// TODO: what if this a reserved keyword?
		// TODO: what if it's null/NULL? not even true/false is identified as a literal
		return &Expression{
			etype: exprIdentifier,
			value: expr.(*ast.Ident).Name,
		}, nil
	case *ast.BasicLit:
		// TODO: do we need to recheck this with our own type parsers?
		node := expr.(*ast.BasicLit)
		var etype exprType
		switch node.Kind {
		case token.INT:
			etype = exprLiteralInt
		case token.FLOAT:
			etype = exprLiteralFloat
		case token.CHAR:
			// TODO: do we need to truncate the value to get rid of the apostrophes? What if there are escaped apostrophes within?
			etype = exprLiteralString
		default:
			return nil, fmt.Errorf("unsupported token: %v", node.Kind)
		}
		return &Expression{
			etype: etype,
			value: node.Value,
		}, nil
	case *ast.BinaryExpr:
		node := expr.(*ast.BinaryExpr)
		var ntype exprType
		switch node.Op {
		case token.ADD:
			ntype = exprAddition
		case token.SUB:
			ntype = exprSubtraction
		case token.MUL:
			ntype = exprMultiplication
		case token.QUO:
			ntype = exprDivision
		case token.EQL:
			ntype = exprEquality
		case token.NEQ:
			ntype = exprNequality
		case token.LSS:
			ntype = exprLessThan
		case token.LEQ:
			ntype = exprLessThanEqual
		case token.GTR:
			ntype = exprGreaterThan
		case token.GEQ:
			ntype = exprGreaterThanEqual
		default:
			return nil, fmt.Errorf("unrecognised operation: %v", node.Op)
		}
		children := make([]*Expression, 2)
		for j, ex := range []ast.Expr{node.X, node.Y} {
			ch, err := convertAstExprToOwnExpr(ex)
			if err != nil {
				return nil, err
			}
			children[j] = ch
		}
		return &Expression{
			etype:    ntype,
			children: children,
		}, nil
	case *ast.CallExpr:
		node := expr.(*ast.CallExpr)
		funName := node.Fun.(*ast.Ident).Name
		var children []*Expression
		for _, arg := range node.Args {
			newc, err := convertAstExprToOwnExpr(arg)
			if err != nil {
				return nil, err
			}
			children = append(children, newc)
		}
		return &Expression{
			etype:    exprFunCall,
			value:    funName,
			children: children,
		}, nil
	default:
		fmt.Println(reflect.TypeOf(expr))
		fset := token.NewFileSet() // positions are relative to fset
		ast.Print(fset, expr)
		return nil, fmt.Errorf("unsupported expression: %v", reflect.TypeOf(expr))
	}
}
