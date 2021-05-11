package expr

import (
	"errors"
	"fmt"
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
func (expr *Expression) ColumnsUsed(schema column.TableSchema) (cols []string) {
	if expr.IsIdentifier() {
		var lookup func(string) (int, column.Schema, error)
		lookup = schema.LocateColumnCaseInsensitive
		if expr.etype == exprIdentifierQuoted {
			lookup = schema.LocateColumn
		}

		_, col, err := lookup(expr.value)
		if err != nil {
			panic(err)
		}
		cols = append(cols, col.Name)
	}
	// normally we'd add all the children to the list, but there's a special case
	// of exprRelabel, where the second child is the relabeled identifier (not a column)
	limit := len(expr.children)
	if expr.etype == exprRelabel {
		limit = 1
	}
	for _, ch := range expr.children[:limit] {
		cols = append(cols, ch.ColumnsUsed(schema)...)
	}
	sort.Strings(cols)
	return dedupeSortedStrings(cols) // so that e.g. a*b - a will yield [a, b]
}

func ColumnsUsed(schema column.TableSchema, exprs ...*Expression) []string {
	var cols []string
	for _, expr := range exprs {
		cols = append(cols, expr.ColumnsUsed(schema)...)
	}
	sort.Strings(cols)
	return dedupeSortedStrings(cols)
}

func (expr *Expression) ReturnType(ts column.TableSchema) (column.Schema, error) {
	schema := column.Schema{}
	switch {
	case expr.etype == exprRelabel:
		if !expr.children[1].IsIdentifier() {
			return schema, errInvalidLabel
		}
		schema.Name = string(expr.children[1].value) // cannot use .String, because quoted identifiers contain quotes
		tschema, err := expr.children[0].ReturnType(ts)
		if err != nil {
			return schema, err
		}
		schema.Dtype = tschema.Dtype
		schema.Nullable = tschema.Nullable
	case expr.etype == exprUnaryMinus:
		ch, err := expr.children[0].ReturnType(ts)
		if err != nil {
			return schema, err
		}
		// TODO/ARCH: we check for numerical types in various places, unify it
		if !(ch.Dtype == column.DtypeInt || ch.Dtype == column.DtypeFloat) {
			return schema, errTypeMismatch
		}

		schema.Dtype = ch.Dtype
		schema.Nullable = ch.Nullable
	case expr.etype == exprNot:
		ch, err := expr.children[0].ReturnType(ts)
		if err != nil {
			return schema, err
		}
		if ch.Dtype != column.DtypeBool {
			return schema, errTypeMismatch
		}

		schema.Dtype = ch.Dtype
		schema.Nullable = ch.Nullable
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
			if t1.Dtype == column.DtypeNull {
				schema.Dtype = t2.Dtype
			}
			// for mixed use cases, always resolve it as a float (1 - 2.0 = -1.0)
			// also division can never result in an integer
			if (t1.Dtype == column.DtypeFloat || t2.Dtype == column.DtypeFloat) || expr.etype == exprDivision {
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
		var (
			col column.Schema
			err error
		)
		if expr.etype == exprIdentifierQuoted {
			_, col, err = ts.LocateColumn(expr.value)
		} else {
			_, col, err = ts.LocateColumnCaseInsensitive(expr.value)
		}
		if err != nil {
			return schema, err
		}
		schema = column.Schema{
			Name:     col.Name,
			Dtype:    col.Dtype,
			Nullable: col.Nullable,
		}
	default:
		return schema, fmt.Errorf("%w: expression %v cannot be resolved", errReturnTypeNotInferred, expr)
	}
	if schema.Name == "" {
		schema.Name = expr.String()
	}
	return schema, nil
}

// now, all function return types are centralised here, but it should probably be embedded in individual functions'
// definitions - we'll need to have some structs in place (for state management in aggregating funcs), so those
// could have methods like `ReturnType(args)` and `IsValid(args)`, `IsAggregating` etc.
// also, should we make multiplication, inequality etc. just functions like nullif or coalesce? That would allow us
// to fold all the functionality of eval() into a (recursive) function call
// TODO: make sure that these return types are honoured in aggregators' resolvers
// TODO: check input types (how will that square off with implementations?)
func funCallReturnType(funName string, argTypes []column.Schema) (column.Schema, error) {
	schema := column.Schema{}
	switch funName {
	case "count":
		if len(argTypes) > 1 {
			return schema, errWrongNumberofArguments
		}
		schema.Dtype = column.DtypeInt
		schema.Nullable = false
	case "min", "max":
		if len(argTypes) != 1 {
			return schema, errWrongNumberofArguments
		}
		schema.Dtype = argTypes[0].Dtype
		schema.Nullable = argTypes[0].Nullable
	case "sum":
		if len(argTypes) != 1 {
			return schema, errWrongNumberofArguments
		}
		// ARCH: isNumericType or something?
		if argTypes[0].Dtype != column.DtypeFloat && argTypes[0].Dtype != column.DtypeInt {
			return schema, errWrongArgumentType
		}
		schema.Dtype = argTypes[0].Dtype
		// ARCH: we can't do sum(bool), because a boolean aggregator can't have internal state in ints yet
		// if argTypes[0].Dtype == column.DtypeBool {
		// 	schema.Dtype = column.DtypeInt
		// }
		schema.Nullable = argTypes[0].Nullable
	case "avg":
		if len(argTypes) != 1 {
			return schema, errWrongNumberofArguments
		}
		// TODO(next): check arg for a numeric type (and fix where we mention "isNumericType")
		// and do this for sin/cos etc.
		schema.Dtype = column.DtypeFloat // average of integers will be a float
		schema.Nullable = argTypes[0].Nullable
	case "sin", "cos", "tan", "asin", "acos", "atan", "sinh", "cosh", "tanh", "sqrt", "exp", "exp2", "log", "log2", "log10":
		if len(argTypes) != 1 {
			return schema, errWrongNumberofArguments
		}
		schema.Dtype = column.DtypeFloat
		schema.Nullable = true
	case "round":
		if len(argTypes) == 0 || len(argTypes) > 2 {
			return schema, errWrongNumberofArguments
		}
		// OPTIM: in case len(argTypes) == 1 && DtypeInt, we could make this a noop
		schema.Dtype = column.DtypeFloat
		schema.Nullable = argTypes[0].Nullable
	case "nullif":
		if len(argTypes) != 2 {
			return schema, errWrongNumberofArguments
		}
		schema.Dtype = argTypes[0].Dtype
		schema.Nullable = true // even if the nullif condition is never met, I think it's fair to set it as nullable
	case "coalesce":
		if len(argTypes) == 0 {
			return schema, errWrongNumberofArguments
		}
		// OPTIM: we can optimise this away if len(argTypes) == 1
		types := make([]column.Dtype, 0, len(argTypes))
		nullable := true
		for _, el := range argTypes {
			types = append(types, el.Dtype)
			// OPTIM: we can prune all the arguments that come after the first non-nullable
			// we can't prune it just yet - we could have an invalid call (e.g. coalesce(int, float, string))
			// but we can note the position of the first non-nullable arg
			if !el.Nullable {
				nullable = false
			}
		}
		candidate, err := coalesceType(types...)
		if err != nil {
			return schema, err
		}
		schema.Dtype = candidate
		schema.Nullable = nullable
	case "trim", "lower", "upper":
		// ARCH: no support for TRIM(foo, 'chars') yet
		if len(argTypes) != 1 {
			return schema, errWrongNumberofArguments
		}
		if argTypes[0].Dtype != column.DtypeString {
			return schema, errWrongArgumentType
		}
		schema.Dtype = column.DtypeString
		schema.Nullable = argTypes[0].Nullable
	default:
		return schema, fmt.Errorf("unsupported function: %v", funName)
	}

	return schema, nil
}
