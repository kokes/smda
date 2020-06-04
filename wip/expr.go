// TODO: explain how this differs from the tokeniser branch
package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"log"
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

type Expression struct {
	// children []*Expression
	// value []byte/string
}

// limitations:
// - cannot use this for full query parsing, just expressions
// - cannot do count(*) and other syntactically problematic expressions (also ::)
// - limited use of = - we might use '==' for all equality for now and later switch to SQL's '='
//   - or we might silently promote any '=' to '==' (but only outside of strings...)
func ParseExpr(s string) (Projection, error) {
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

func main() {
	// tree, err := ParseExpr("123*bak + nullif(\"foo\", 'abc')")
	tree, err := ParseExpr("(bak - 4) == (bar+3)")
	if err != nil {
		log.Fatal(err)
	}
	_ = tree
}
