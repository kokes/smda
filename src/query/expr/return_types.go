package expr

import (
	"errors"
	"sort"

	"github.com/kokes/smda/src/column"
)

var errTypeMismatch = errors.New("expecting compatible types")
var errNoTypes = errors.New("expecting at least one column")
var errWrongNumberofArguments = errors.New("wrong number arguments passed to a function")
var errWrongArgumentType = errors.New("wrong argument type passed to a function")
var errReturnTypeNotInferred = errors.New("cannot infer return type of expression")
var errInvalidLabel = errors.New("cannot relabel projection")

// should this be in the database package?
func comparableTypes(t1, t2 column.Dtype) bool {
	if t1 == t2 {
		return true
	}
	if (t1 == column.DtypeFloat && t2 == column.DtypeInt) || (t2 == column.DtypeFloat && t1 == column.DtypeInt) {
		return true
	}
	// we can compare 1=null or do 4+null
	if (t1 == column.DtypeNull || t2 == column.DtypeNull) && !(t1 == column.DtypeNull && t2 == column.DtypeNull) {
		return true
	}
	return false
}

func coalesceType(types ...column.Dtype) (column.Dtype, error) {
	if len(types) == 0 {
		return column.DtypeInvalid, errNoTypes
	}
	if len(types) == 1 {
		return types[0], nil
	}

	candidate := types[0]
	for _, el := range types[1:] {
		if el == candidate || (el == column.DtypeInt && candidate == column.DtypeFloat) {
			continue
		}
		if el == column.DtypeFloat && candidate == column.DtypeInt {
			candidate = column.DtypeFloat
			continue
		}

		return column.DtypeInvalid, errTypeMismatch
	}
	return candidate, nil
}

func dedupeSortedStrings(s []string) []string {
	if len(s) < 2 {
		return s
	}
	lastVal := s[0]
	currPos := 1
	for _, el := range s[1:] {
		if el == lastVal {
			continue
		}
		s[currPos] = el
		lastVal = el
		currPos++
	}
	return s[:currPos]
}

// ARCH: this panics when a given column is not in the schema, but since we already validated
// this schema during the ReturnType call, we should be fine. It's still a bit worrying that
// we might panic though.
func ColumnsUsed(expr Expression, schema column.TableSchema) (cols []string) {
	if idf, ok := expr.(*Identifier); ok {
		var lookup func(string) (int, column.Schema, error)
		lookup = schema.LocateColumnCaseInsensitive
		if idf.quoted {
			lookup = schema.LocateColumn
		}

		_, col, err := lookup(idf.name)
		if err != nil {
			panic(err)
		}
		cols = append(cols, col.Name)
	}
	// normally we'd add all the children to the list, but there's a special case
	// of exprRelabel, where the second child is the relabeled identifier (not a column)
	children := expr.Children()
	limit := len(children)
	if ex, ok := expr.(*Infix); ok && ex.operator == tokenAs {
		limit = 1
	}
	for _, ch := range children[:limit] {
		cols = append(cols, ColumnsUsed(ch, schema)...)
	}
	sort.Strings(cols)
	return dedupeSortedStrings(cols) // so that e.g. a*b - a will yield [a, b]
}

func ColumnsUsedMultiple(schema column.TableSchema, exprs ...Expression) []string {
	var cols []string
	for _, expr := range exprs {
		cols = append(cols, ColumnsUsed(expr, schema)...)
	}
	sort.Strings(cols)
	return dedupeSortedStrings(cols)
}
