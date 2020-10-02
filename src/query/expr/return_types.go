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
func comparableTypes(t1, t2 column.Dtype) bool {
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

	// TODO: what about a) quoted identifiers and b) case insensitivity in normal identifiers?
	//       the problem is we don't have the schema passed in here
	if expr.etype == exprIdentifier {
		cols = append(cols, expr.value)
	}
	for _, ch := range expr.children {
		cols = append(cols, ch.ColumnsUsed()...)
	}
	sort.Strings(cols)
	return dedupeSortedStrings(cols) // so that e.g. a*b - a will yield [a, b]
}

func ColumnsUsed(exprs ...*Expression) []string {
	var cols []string
	for _, expr := range exprs {
		cols = append(cols, expr.ColumnsUsed()...)
	}
	sort.Strings(cols)
	return dedupeSortedStrings(cols)
}

// TODO: will we define the name? As some sort of a composite of the actions taken?
// does this even need to return errors? If we always call IsValid outside of this, then this will
// always return a type - one issue with the current implementation is that isvalid gets called recursively
// once at the top and then for all the children again (because we call ReturnType on the children)
func (expr *Expression) ReturnType(ts database.TableSchema) (column.Schema, error) {
	schema := column.Schema{}
	switch {
	case expr.IsLiteral():
		schema.Nullable = false // ARCH: still no consensus whether null columns are nullable
		switch expr.etype {
		case exprLiteralInt:
			schema.Dtype = column.DtypeInt
		case exprLiteralFloat:
			schema.Dtype = column.DtypeFloat
		case exprLiteralBool:
			schema.Dtype = column.DtypeBool
		case exprLiteralString:
			schema.Dtype = column.DtypeString
		case exprLiteralNull:
			schema.Dtype = column.DtypeNull
		default:
			return schema, fmt.Errorf("literal %v not supported", expr)
		}
	case expr.IsOperator():
		t1, err := expr.children[0].ReturnType(ts)
		if err != nil {
			return schema, err
		}
		t2, err := expr.children[1].ReturnType(ts)
		if err != nil {
			return schema, err
		}
		switch {
		case expr.IsOperatorBoolean():
			if !(t1.Dtype == column.DtypeBool && t2.Dtype == column.DtypeBool) {
				return schema, fmt.Errorf("AND/OR clauses require both sides to be booleans: %w", errTypeMismatch)
			}
			schema.Dtype = column.DtypeBool
			schema.Nullable = t1.Nullable || t2.Nullable
		case expr.IsOperatorComparison():
			if !comparableTypes(t1.Dtype, t2.Dtype) {
				return schema, errTypeMismatch
			}
			schema.Dtype = column.DtypeBool
			schema.Nullable = t1.Nullable || t2.Nullable
		case expr.IsOperatorMath():
			if !comparableTypes(t1.Dtype, t2.Dtype) {
				return schema, errTypeMismatch
			}
			schema.Dtype = t1.Dtype
			// for mixed use cases, always resolve it as a float (1 - 2.0 = -1.0)
			// also division can never result in an integer
			if t1.Dtype != t2.Dtype || expr.etype == exprDivision {
				schema.Dtype = column.DtypeFloat
			}
			schema.Nullable = t1.Nullable || t2.Nullable
		default:
			return schema, fmt.Errorf("operator type %v not supported", expr.etype)
		}
	case expr.etype == exprFunCall:
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
	case expr.IsIdentifier():
		// TODO: what about the quoted identifier vs. unquoted (case sensitivity)
		_, col, err := ts.LocateColumn(expr.value)
		if err != nil {
			return schema, err
		}
		schema = column.Schema{
			Name:     col.Name,
			Dtype:    col.Dtype,
			Nullable: col.Nullable,
		}
	default:
		return schema, fmt.Errorf("expression %v cannot be resolved", expr)
	}
	return schema, nil
}

// now, all function return types are centralised here, but it should probably be embedded in individual functions'
// definitions - we'll need to have some structs in place (for state management in aggregating funcs), so those
// could have methods like `ReturnType(args)` and `IsValid(args)`, `IsAggregating` etc.
// also, should we make multiplication, inequality etc. just functions like nullif or coalesce? That would allow us
// to fold all the functionality of eval() into a (recursive) function call
// TODO: make sure that these return types are honoured in aggregators' resolvers
func funCallReturnType(funName string, argTypes []column.Schema) (column.Schema, error) {
	schema := column.Schema{}
	switch funName {
	case "count":
		schema.Dtype = column.DtypeInt
		schema.Nullable = false
	case "min", "max":
		schema.Dtype = argTypes[0].Dtype
		schema.Nullable = argTypes[0].Nullable
	case "sum":
		schema.Dtype = argTypes[0].Dtype
		// ARCH: we can't do sum(bool), because a boolean aggregator can't have internal state in ints yet
		// if argTypes[0].Dtype == column.DtypeBool {
		// 	schema.Dtype = column.DtypeInt
		// }
		schema.Nullable = argTypes[0].Nullable
	case "avg":
		schema.Dtype = column.DtypeFloat // average of integers will be a float
		schema.Nullable = argTypes[0].Nullable
	case "sin", "cos", "tan", "asin", "acos", "atan", "sinh", "cosh", "tanh", "sqrt", "exp", "exp2", "log", "log2", "log10":
		schema.Dtype = column.DtypeFloat
		schema.Nullable = true
	case "round":
		// TODO: check the number of params
		// disallow anything but literals in the second argument
		schema.Dtype = column.DtypeFloat
		schema.Nullable = argTypes[0].Nullable
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
