package expr

import (
	"go/ast"
	"go/parser"
	"go/token"
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

// just an implementation of Projection - we might merge the two eventually
type Expression struct {
	// children []*Expression
	// value []byte/string
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
func ParseExpr(s string) (Projection, error) {
	// toks, err := tokeniseString(s) // helper function, TBA
	// toks = compatToks(toks)
	// s2 := stringify(toks) // strings.Builder etc. - will need a stringer for type tok
	tr, err := parser.ParseExpr(s)

	// we are fine with illegal rune literals - because we need e.g. 'ahoy' as literal strings
	if err != nil && !strings.HasSuffix(err.Error(), "illegal rune literal") {
		return nil, err
	}

	// switch tree.(type) - if the base is ast.BasicLit or ast.Ident, we can exit early

	fs := token.NewFileSet()
	ast.Print(fs, tr)

	return nil, nil
}

// func main() {
// 	// tree, err := ParseExpr("123*bak + nullif(\"foo\", 'abc')")
// 	tree, err := ParseExpr("(bak - 4) == (bar+3)")
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	_ = tree
// }
