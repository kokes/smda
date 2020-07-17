package expr

import (
	"errors"
	"fmt"
	"sort"

	"github.com/kokes/smda/src/column"
	"github.com/kokes/smda/src/database"
)

var errChildrenNotNil = errors.New("expecting an expression to have nil children nodes")
var errChildrenNotTwo = errors.New("expecting an expression to have two children")
var errTypeMismatch = errors.New("expecting compatible types")

// should this be in the database package?
func compatibleTypes(t1, t2 column.Dtype) bool {
	if t1 == t2 {
		return true
	}
	if (t1 == column.DtypeFloat && t2 == column.DtypeInt) || (t2 == column.DtypeFloat && t1 == column.DtypeInt) {
		return true
	}
	return false
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

func (expr *Expression) ColumnsUsed() []string {
	var cols []string

	if expr.etype == exprIdentifier {
		cols = append(cols, expr.value)
	}
	for _, ch := range expr.children {
		cols = append(cols, ch.ColumnsUsed()...)
	}
	sort.Strings(cols)
	return dedupeSortedStrings(cols) // so that e.g. a*b - a will yield [a, b]
}

func (expr *Expression) IsValid(ts database.TableSchema) error {
	switch expr.etype {
	case exprIdentifier:
		// TODO: test value?
		if expr.children != nil {
			return errChildrenNotNil
		}
	case exprLiteralInt, exprLiteralFloat, exprLiteralString, exprLiteralBool:
		// TODO: test value
		if expr.children != nil {
			return errChildrenNotNil
		}
	case exprEquality, exprNequality, exprGreaterThan, exprGreaterThanEqual, exprLessThan, exprLessThanEqual,
		exprAddition, exprSubtraction, exprDivision, exprMultiplication:
		if len(expr.children) != 2 {
			return errChildrenNotTwo
		}
		t1, err := expr.children[0].ReturnType(ts)
		if err != nil {
			return err
		}
		t2, err := expr.children[1].ReturnType(ts)
		if err != nil {
			return err
		}
		// TODO: this might be an issue for e.g. colDate = '2020-02-22' - where we'd like to implicitly convert the literal
		//       to a date, so we may need to extend this to avoid explicit casting
		if !compatibleTypes(t1.Dtype, t2.Dtype) {
			return errTypeMismatch
		}
	case exprFunCall:
		// TODO: check the function exists?
		// also check its arguments (e.g. nullif needs exactly two)
	default:
		return fmt.Errorf("unsupported expression type for validity checks: %v", expr.etype)
	}
	return nil
}

// TODO: will we define the name? As some sort of a composite of the actions taken?
// does this even need to return errors? If we always call IsValid outside of this, then this will
// always return a type - one issue with the current implementation is that isvalid gets called recursively
// once at the top and then for all the children again (because we call ReturnType on the children)
func (expr *Expression) ReturnType(ts database.TableSchema) (column.Schema, error) {
	schema := column.Schema{}
	if err := expr.IsValid(ts); err != nil {
		return schema, err
	}
	switch expr.etype {
	case exprLiteralInt:
		schema.Dtype = column.DtypeInt
		schema.Nullable = false
	case exprLiteralFloat:
		schema.Dtype = column.DtypeFloat
		schema.Nullable = false
	case exprLiteralBool:
		schema.Dtype = column.DtypeBool
		schema.Nullable = false
	case exprLiteralString:
		schema.Dtype = column.DtypeString
		schema.Nullable = false

	case exprFunCall:
		var argTypes []column.Schema
		for _, child := range expr.children {
			ctype, err := child.ReturnType(ts)
			if err != nil {
				return schema, err
			}
			argTypes = append(argTypes, ctype)
		}
		fschema, err := funCallReturnType(expr.value, argTypes)
		if err != nil {
			return schema, err
		}
		schema = fschema
	case exprIdentifier:
		_, col, err := ts.LocateColumn(expr.value)
		if err != nil {
			return schema, err
		}
		schema = column.Schema{
			Name:     col.Name,
			Dtype:    col.Dtype,
			Nullable: col.Nullable,
		}

	case exprEquality, exprNequality, exprGreaterThan, exprGreaterThanEqual, exprLessThan, exprLessThanEqual:
		schema.Dtype = column.DtypeBool
		c1, err := expr.children[0].ReturnType(ts)
		if err != nil {
			return schema, err
		}
		c2, err := expr.children[1].ReturnType(ts)
		if err != nil {
			return schema, err
		}
		schema.Nullable = c1.Nullable || c2.Nullable
	case exprAddition, exprSubtraction, exprDivision, exprMultiplication:
		c1, err := expr.children[0].ReturnType(ts)
		if err != nil {
			return schema, err
		}
		c2, err := expr.children[1].ReturnType(ts)
		if err != nil {
			return schema, err
		}
		schema.Dtype = column.DtypeFloat
		// int operations (apart from division) will result back in ints
		if expr.etype != exprDivision && (c1.Dtype == column.DtypeInt && c2.Dtype == column.DtypeInt) {
			schema.Dtype = column.DtypeInt
		}
		schema.Nullable = c1.Nullable || c2.Nullable
	default:
		return schema, fmt.Errorf("TODO: %v", expr.etype)
	}
	return schema, nil
}

// now, all function return types are centralised here, but it should probably be embedded in individual functions'
// definitions - we'll need to have some structs in place (for state management in aggregating funcs), so those
// could have methods like `ReturnType(args)` and `IsValid(args)`, `IsAggregating` etc.
// note that a call to IsValid MUST precede this
// also, should we make multiplication, inequality etc. just functions like nullif or coalesce? That would allow us
// to fold all the functionality of eval() into a (recursive) function call
func funCallReturnType(funName string, argTypes []column.Schema) (column.Schema, error) {
	schema := column.Schema{}
	switch funName {
	case "count":
		schema.Dtype = column.DtypeInt
		schema.Nullable = false // can we get somehow get empty groups and thus nulls in counts?
	case "nullif":
		schema.Dtype = argTypes[0].Dtype // TODO: add nullif() to tests to ensure that we catch it before this and don't panic
		schema.Nullable = true           // even if the nullif condition is never met, I think it's fair to set it as nullable
	// case "coalesce":
	// we'll need to figure out how to deal with the whole number-like type compatibility (e.g. if there's at least
	// one float, it's a float - but that will change in the future if we add decimals)
	// same issue in multiplication and other operations
	// trying something with compatibleTypes()
	default:
		return schema, fmt.Errorf("unsupported function: %v", funName)
	}

	return schema, nil
}
