package expr

import (
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strings"

	"github.com/kokes/smda/src/column"
)

var errFunctionDoesNotExist = errors.New("function does not exist")

type exprType uint8

const (
	exprInvalid exprType = iota
	exprIdentifier
	exprIdentifierQuoted
	exprAnd
	exprOr
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
	exprLiteralBool
	exprLiteralString
	exprLiteralNull
	exprFunCall
)

// ARCH: is this used anywhere?
func (etype exprType) String() string {
	switch etype {
	case exprInvalid:
		return "Invalid"
	case exprIdentifier:
		return "Identifier"
	case exprIdentifierQuoted:
		return "QuotedIdentifier"
	case exprAnd:
		return "&&"
	case exprOr:
		return "||"
	case exprAddition:
		return "+"
	case exprSubtraction:
		return "-"
	case exprMultiplication:
		return "*"
	case exprDivision:
		return "/"
	case exprEquality:
		return "="
	case exprNequality:
		return "!="
	case exprLessThan:
		return "<"
	case exprLessThanEqual:
		return "<="
	case exprGreaterThan:
		return ">"
	case exprGreaterThanEqual:
		return ">="
	case exprLiteralInt:
		return "LiteralInt"
	case exprLiteralFloat:
		return "LiteralFloat"
	case exprLiteralBool:
		return "LiteralBool"
	case exprLiteralString:
		return "LiteralString"
	case exprLiteralNull:
		return "LiteralNull"
	case exprFunCall:
		return "FunCall"
	default:
		return "unknown_expression"
	}
}

// isValid(tableSchema) - does it make sense to have this projection like this?
//   - tableSchema = []columnSchema
//   - checks that type are okay and everything
// ReturnType dtype - though we'll have to pass in a schema
// ColumnsUsed []string
// isSimpleton (or something along those lines) - if this projection is just a column or a literal?
//  - we might need a new typedColumn - columnLit{string,int,float,bool}?
type Expression struct {
	etype    exprType
	children []*Expression
	value    string
	evaler   func(...column.Chunk) (column.Chunk, error)
}

func (expr *Expression) String() string {
	switch expr.etype {
	case exprInvalid:
		return "invalid_expression"
	case exprIdentifier:
		return expr.value
	case exprIdentifierQuoted:
		return fmt.Sprintf("\"%s\"", expr.value)
	case exprAddition, exprSubtraction, exprMultiplication, exprDivision, exprEquality,
		exprNequality, exprLessThan, exprLessThanEqual, exprGreaterThan, exprGreaterThanEqual, exprAnd, exprOr:
		return fmt.Sprintf("%s%s%s", expr.children[0], expr.etype, expr.children[1])
	case exprLiteralInt, exprLiteralFloat, exprLiteralBool, exprLiteralString:
		return expr.value
	case exprLiteralNull:
		return "NULL"
	case exprFunCall:
		args := make([]string, 0, len(expr.children))
		for _, ch := range expr.children {
			args = append(args, ch.String())
		}

		return fmt.Sprintf("%s(%s)", expr.value, strings.Join(args, ", "))
	default:
		return "unsupported expression"
	}
}

func (expr *Expression) UnmarshalJSON(data []byte) error {
	if len(data) < 2 || !(data[0] == '"' && data[len(data)-1] == '"') {
		return fmt.Errorf("failed to unmarshal into an Expression, did not receive a quoted string: %s", data)
	}
	sdata := string(data[1 : len(data)-1])
	ex, err := ParseStringExpr(sdata)
	if ex != nil {
		*expr = *ex
	}
	return err
}

func (expr *Expression) MarshalJSON() ([]byte, error) {
	return json.Marshal(expr.String())
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
// when building our own parser, consider:
// isPrecedence: get inspired: https://golang.org/src/go/token/token.go?s=4316:4348#L253
//  - then build an expression parser with precedence built in
func ParseStringExpr(s string) (*Expression, error) {
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
	switch node := expr.(type) {
	case *ast.Ident:
		// TODO: what if this a reserved keyword? (we don't have any just yet)
		value := node.Name

		// not the same set of values as in parseBool, because we only want true/false (upper/lower) in literal expressions
		if value == "true" || value == "TRUE" || value == "false" || value == "FALSE" {
			return &Expression{
				etype: exprLiteralBool,
				value: fmt.Sprintf("%v", value == "true" || value == "TRUE"),
			}, nil
		}
		if value == "null" || value == "NULL" {
			return &Expression{
				etype: exprLiteralNull,
			}, nil
		}

		return &Expression{
			etype: exprIdentifier,
			value: value,
		}, nil
	case *ast.BasicLit:
		// TODO: do we need to recheck this with our own type parsers?
		var etype exprType
		var value string
		switch node.Kind {
		case token.STRING:
			etype = exprIdentifierQuoted
			value = node.Value[1 : len(node.Value)-1]
		case token.INT:
			etype = exprLiteralInt
			value = node.Value
		case token.FLOAT:
			etype = exprLiteralFloat
			value = node.Value
		case token.CHAR:
			etype = exprLiteralString
			value = node.Value[1 : len(node.Value)-1] // trim apostrophes
			value = strings.ReplaceAll(value, "\\'", "'")
		default:
			return nil, fmt.Errorf("unsupported token: %v", node.Kind)
		}
		return &Expression{
			etype: etype,
			value: value,
		}, nil
	case *ast.UnaryExpr:
		if node.Op == token.ADD {
			return convertAstExprToOwnExpr(node.X)
		}
		if node.Op != token.SUB {
			return nil, fmt.Errorf("unsupported op: %s", node.Op)
		}

		// simple -1 or -2.4 should be converted into a literal
		if x, ok := node.X.(*ast.BasicLit); ok {
			var etype exprType
			switch x.Kind {
			case token.INT:
				etype = exprLiteralInt
			case token.FLOAT:
				etype = exprLiteralFloat
			default:
				return nil, fmt.Errorf("unsupported token for unary expressions: %v", x.Kind)
			}
			return &Expression{
				etype: etype,
				value: fmt.Sprintf("-%s", x.Value),
			}, nil
		}

		// all the other unary expressions, e.g. -foo, -(2 - bar) should be extended to (-1)*something
		ch, err := convertAstExprToOwnExpr(node.X)
		if err != nil {
			return nil, err
		}
		return &Expression{
			etype: exprMultiplication,
			children: []*Expression{
				{etype: exprLiteralInt, value: "-1"},
				ch,
			},
		}, nil
	case *ast.BinaryExpr:
		var ntype exprType
		switch node.Op {
		case token.LAND:
			ntype = exprAnd
		case token.LOR:
			ntype = exprOr
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
		funName := node.Fun.(*ast.Ident).Name
		fnc, ok := column.FuncProj[funName]
		if !ok {
			return nil, fmt.Errorf("%w: %s", errFunctionDoesNotExist, funName)
		}
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
			evaler:   fnc,
		}, nil
	case *ast.ParenExpr:
		// I think we can just take what's in it and treat it as a node - since our evaluation/encapsulation
		// treats it as a paren expression anyway, right?
		return convertAstExprToOwnExpr(node.X)
	default:
		fmt.Println(reflect.TypeOf(expr))
		fset := token.NewFileSet() // positions are relative to fset
		ast.Print(fset, expr)
		return nil, fmt.Errorf("unsupported expression: %v", reflect.TypeOf(expr))
	}
}
