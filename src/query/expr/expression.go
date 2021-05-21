package expr

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/kokes/smda/src/column"
	"github.com/kokes/smda/src/database"
)

var errNoNestedAggregations = errors.New("cannot nest aggregations (e.g. sum(min(a)))")

type Expression interface {
	ReturnType(ts column.TableSchema) (column.Schema, error)
	String() string
	Children() []Expression
}

func PruneFunctionCalls(ex Expression) {
	if f, ok := ex.(*Function); ok {
		f.aggregator = nil
		f.aggregatorFactory = nil
		f.evaler = nil
	}
	for _, ch := range ex.Children() {
		PruneFunctionCalls(ch)
	}
}

type ExpressionList []Expression

// Query describes what we want to retrieve from a given dataset
// There are basically four places you need to edit (and test!) in order to extend this:
// 1) The engine itself needs to support this functionality (usually a method on Dataset or column.Chunk)
// 2) The query method has to be able to translate query parameters to the engine
// 3) The query endpoint handler needs to be able to process the incoming body
//    to the Query struct (the Unmarshaler should mostly take care of this)
// 4) The HTML/JS frontend needs to incorporate this in some way
type Query struct {
	Select  ExpressionList              `json:"select,omitempty"`
	Dataset *database.DatasetIdentifier `json:"dataset"`
	// ARCH: this is quite hacky - we know Filter can only be a single Expression,
	// but we cannot unmarshal Expressions as they are interfaces
	Filter    ExpressionList `json:"filter,omitempty"`
	Aggregate ExpressionList `json:"aggregate,omitempty"`
	Order     ExpressionList `json:"order,omitempty"`
	Limit     *int           `json:"limit,omitempty"`
	// TODO: PAFilter (post-aggregation filter, == having) - check how it behaves without aggregations elsewhere
}

// this stringer is tested in the parser
func (q Query) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("SELECT %s", q.Select))
	// ARCH: preparing for queries without FROM clauses
	if q.Dataset != nil {
		sb.WriteString(fmt.Sprintf(" FROM %s", q.Dataset))
	}
	if q.Filter != nil {
		sb.WriteString(fmt.Sprintf(" WHERE %s", q.Filter))
	}
	if q.Aggregate != nil {
		sb.WriteString(fmt.Sprintf(" GROUP BY %s", q.Aggregate))
	}
	if q.Order != nil {
		sb.WriteString(fmt.Sprintf(" ORDER BY %s", q.Order))
	}
	if q.Limit != nil {
		sb.WriteString(fmt.Sprintf(" LIMIT %d", *q.Limit))
	}

	return sb.String()
}

func InitAggregator(fun *Function, schema column.TableSchema) error {
	var rtypes []column.Dtype
	for _, ch := range fun.args {
		rtype, err := ch.ReturnType(schema)
		if err != nil {
			return err
		}
		rtypes = append(rtypes, rtype.Dtype)
	}
	aggregator, err := fun.aggregatorFactory(rtypes...)
	if err != nil {
		return err
	}
	fun.aggregator = aggregator
	return nil
}

func AggExpr(expr Expression) ([]*Function, error) {
	var ret []*Function
	found := false
	// ARCH: we used to test `expr.evaler == nil` in the second condition... better?
	fun, ok := expr.(*Function)
	if ok && fun.aggregatorFactory != nil {
		ret = append(ret, fun)
		found = true
	}
	for _, ch := range expr.Children() {
		ach, err := AggExpr(ch)
		if err != nil {
			return nil, err
		}
		if ach != nil {
			if found {
				return nil, errNoNestedAggregations
			}
			ret = append(ret, ach...)
		}
	}
	return ret, nil
}

// cannot have interface pointer receivers
func FromJSON(data []byte) (Expression, error) {
	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	return ParseStringExpr(raw)
}

func ToJSON(expr Expression) ([]byte, error) {
	return json.Marshal(expr.String())
}

// ARCH: this is a bit contentious - our []*Expression aka ExpressionList (un)marshals
// as a "expr, expr2", NOT as "[]*Expression{expr, expr2}"
func (exprs *ExpressionList) UnmarshalJSON(data []byte) error {
	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	ex, err := ParseStringExprs(raw)
	if ex != nil {
		*exprs = ex
	}
	return err
}

func (exprs ExpressionList) String() string {
	var buf bytes.Buffer
	for j, expr := range exprs {
		buf.WriteString(expr.String())
		if j < len(exprs)-1 {
			buf.WriteString(", ")
		}
	}
	return buf.String()
}

func (exprs ExpressionList) MarshalJSON() ([]byte, error) {
	return json.Marshal(exprs.String())
}

type Identifier struct {
	quoted bool
	name   string
}

func (ex *Identifier) ReturnType(ts column.TableSchema) (column.Schema, error) {
	if ex.quoted {
		_, col, err := ts.LocateColumn(ex.name)
		return col, err
	}
	_, col, err := ts.LocateColumnCaseInsensitive(ex.name)
	return col, err
}

func (ex *Identifier) String() string {
	if !ex.quoted {
		return ex.name
	}
	return fmt.Sprintf("\"%s\"", ex.name)
}
func (ex *Identifier) Children() []Expression {
	return nil
}

type Integer struct {
	value int64
}

func (ex *Integer) ReturnType(ts column.TableSchema) (column.Schema, error) {
	return column.Schema{
		Name:     ex.String(),
		Dtype:    column.DtypeInt,
		Nullable: false,
	}, nil
}
func (ex *Integer) String() string {
	return fmt.Sprintf("%v", ex.value)
}
func (ex *Integer) Children() []Expression {
	return nil
}

type Float struct {
	value float64
}

func (ex *Float) ReturnType(ts column.TableSchema) (column.Schema, error) {
	return column.Schema{
		Name:     ex.String(),
		Dtype:    column.DtypeFloat,
		Nullable: false,
	}, nil
}
func (ex *Float) String() string {
	return fmt.Sprintf("%v", ex.value)
}
func (ex *Float) Children() []Expression {
	return nil
}

type Bool struct {
	value bool
}

func (ex *Bool) ReturnType(ts column.TableSchema) (column.Schema, error) {
	return column.Schema{
		Name:     ex.String(),
		Dtype:    column.DtypeBool,
		Nullable: false,
	}, nil
}
func (ex *Bool) String() string {
	if ex.value {
		return "TRUE"
	}
	return "FALSE"
}
func (ex *Bool) Children() []Expression {
	return nil
}

type String struct {
	value string
}

func (ex *String) ReturnType(ts column.TableSchema) (column.Schema, error) {
	return column.Schema{
		Name:     ex.String(),
		Dtype:    column.DtypeString,
		Nullable: false,
	}, nil
}
func (ex *String) String() string {
	// TODO: what about literals with apostrophes in them? escape them
	return fmt.Sprintf("'%s'", ex.value)
}
func (ex *String) Children() []Expression {
	return nil
}

type Null struct{}

func (ex *Null) ReturnType(ts column.TableSchema) (column.Schema, error) {
	return column.Schema{
		Name:     "NULL",
		Dtype:    column.DtypeNull,
		Nullable: false,
	}, nil
}
func (ex *Null) String() string {
	return "NULL"
}
func (ex *Null) Children() []Expression {
	return nil
}

type Function struct {
	name              string
	args              []Expression
	evaler            func(...column.Chunk) (column.Chunk, error)
	aggregator        *column.AggState
	aggregatorFactory func(...column.Dtype) (*column.AggState, error)
}

// NewFunction is one of the very few constructors as we have to do some fiddling here
func NewFunction(name string) (*Function, error) {
	ex := &Function{name: name}
	fncp, ok := column.FuncProj[name]
	if ok {
		ex.evaler = fncp
	} else {
		// if it's not a projection, it must be an aggregator
		// ARCH: cannot initialise the aggregator here, because we don't know
		// the types that go in (and we're already using static dispatch here)
		// TODO/ARCH: but since we've decoupled this from the parser, we might have the schema at hand already!
		//            we just need to remove this `InitFunctionCalls` from ParseStringExpr
		aggfac, err := column.NewAggregator(name)
		if err != nil {
			return nil, err
		}
		ex.aggregatorFactory = aggfac
	}
	return ex, nil
}

// now, all function return types are centralised here, but it should probably be embedded in individual functions'
// definitions - we'll need to have some structs in place (for state management in aggregating funcs), so those
// could have methods like `ReturnType(args)` and `IsValid(args)`, `IsAggregating` etc.
// also, should we make multiplication, inequality etc. just functions like nullif or coalesce? That would allow us
// to fold all the functionality of eval() into a (recursive) function call
// TODO: make sure that these return types are honoured in aggregators' resolvers
// TODO: check input types (how will that square off with implementations?)
func (ex *Function) ReturnType(ts column.TableSchema) (column.Schema, error) {
	schema := column.Schema{Name: ex.String()}
	var argTypes []column.Schema
	for _, child := range ex.args {
		ctype, err := child.ReturnType(ts)
		if err != nil {
			return schema, err
		}
		argTypes = append(argTypes, ctype)
	}
	switch ex.name {
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
	case "left":
		if len(argTypes) != 2 {
			return schema, errWrongNumberofArguments
		}
		if argTypes[0].Dtype != column.DtypeString {
			return schema, errWrongArgumentType
		}
		if argTypes[1].Dtype != column.DtypeInt {
			return schema, errWrongArgumentType
		}
		schema.Dtype = column.DtypeString
		schema.Nullable = argTypes[0].Nullable
	default:
		return schema, fmt.Errorf("unsupported function: %v", ex.name)
	}

	return schema, nil
}
func (ex *Function) String() string {
	args := make([]string, 0, len(ex.args))
	for _, ch := range ex.args {
		args = append(args, ch.String())
	}

	return fmt.Sprintf("%s(%s)", ex.name, strings.Join(args, ", "))
}
func (ex *Function) Children() []Expression {
	return ex.args
}

type Prefix struct {
	operator tokenType
	right    Expression
}

func (ex *Prefix) ReturnType(ts column.TableSchema) (column.Schema, error) {
	schema := column.Schema{Name: ex.String()}
	switch ex.operator {
	case tokenSub:
		ch, err := ex.right.ReturnType(ts)
		if err != nil {
			return schema, err
		}
		// TODO/ARCH: we check for numerical types in various places, unify it
		if !(ch.Dtype == column.DtypeInt || ch.Dtype == column.DtypeFloat) {
			return schema, errTypeMismatch
		}

		schema.Dtype = ch.Dtype
		schema.Nullable = ch.Nullable

	case tokenNot:
		ch, err := ex.right.ReturnType(ts)
		if err != nil {
			return schema, err
		}
		if ch.Dtype != column.DtypeBool {
			return schema, errTypeMismatch
		}

		schema.Dtype = ch.Dtype
		schema.Nullable = ch.Nullable
	}
	return schema, nil
}
func (ex *Prefix) String() string {
	space := " "
	if ex.operator == tokenSub {
		space = ""
	}
	op := token{ttype: ex.operator} // TODO: this is a hack, because we don't have ttype stringers
	return fmt.Sprintf("%s%s%s", op, space, ex.right)
}
func (ex *Prefix) Children() []Expression {
	return []Expression{ex.right}
}

type Infix struct {
	operator tokenType
	left     Expression
	right    Expression
}

func (ex *Infix) ReturnType(ts column.TableSchema) (column.Schema, error) {
	// TODO(next): check out all the ReturnTypes in here and see if they implement this correctly,
	// we had columns without names on multiple occasions (oh and test all this)
	schema := column.Schema{Name: ex.String()}
	t1, err := ex.left.ReturnType(ts)
	if err != nil {
		return schema, err
	}
	t2, err := ex.right.ReturnType(ts)
	if err != nil {
		return schema, err
	}
	switch ex.operator {
	case tokenAnd, tokenOr:
		if !(t1.Dtype == column.DtypeBool && t2.Dtype == column.DtypeBool) {
			return schema, fmt.Errorf("AND/OR clauses require both sides to be booleans: %w", errTypeMismatch)
		}
		schema.Dtype = column.DtypeBool
		schema.Nullable = t1.Nullable || t2.Nullable
	case tokenEq, tokenIs, tokenNeq, tokenLt, tokenGt, tokenLte, tokenGte:
		if !comparableTypes(t1.Dtype, t2.Dtype) {
			return schema, errTypeMismatch
		}
		schema.Dtype = column.DtypeBool
		schema.Nullable = t1.Nullable || t2.Nullable
	case tokenAdd, tokenSub, tokenMul, tokenQuo:
		if !comparableTypes(t1.Dtype, t2.Dtype) {
			return schema, errTypeMismatch
		}
		schema.Dtype = t1.Dtype
		if t1.Dtype == column.DtypeNull {
			schema.Dtype = t2.Dtype
		}
		// for mixed use cases, always resolve it as a float (1 - 2.0 = -1.0)
		// also division can never result in an integer
		if (t1.Dtype == column.DtypeFloat || t2.Dtype == column.DtypeFloat) || ex.operator == tokenQuo {
			schema.Dtype = column.DtypeFloat
		}
		schema.Nullable = t1.Nullable || t2.Nullable
	default:
		return schema, fmt.Errorf("operator type %v not supported", ex.operator)
	}
	return schema, nil
}
func (ex *Infix) String() string {
	op := token{ttype: ex.operator}.String() // TODO: this is a hack, because we don't have ttype stringers
	// TODO(next): test for this (e.g. `foo is 12` => `foo IS 12`)
	if ex.operator == tokenAnd || ex.operator == tokenOr || ex.operator == tokenIs {
		op = fmt.Sprintf(" %s ", op)
	}
	return fmt.Sprintf("%s%s%s", ex.left, op, ex.right)
}
func (ex *Infix) Children() []Expression {
	return []Expression{ex.left, ex.right}
}

type Relabel struct {
	inner Expression
	Label string // exporting it, because there's no other way of getting to it
}

func (ex *Relabel) ReturnType(ts column.TableSchema) (column.Schema, error) {
	schema, err := ex.inner.ReturnType(ts)
	if err != nil {
		return schema, err
	}
	schema.Name = ex.Label
	return schema, nil
}

func (ex *Relabel) String() string {
	return fmt.Sprintf("%s AS %s", ex.inner.String(), ex.Label)
}

func (ex *Relabel) Children() []Expression {
	return []Expression{ex.inner}
}

type Parentheses struct {
	inner Expression
}

func (ex *Parentheses) ReturnType(ts column.TableSchema) (column.Schema, error) {
	return ex.inner.ReturnType(ts)
}
func (ex *Parentheses) String() string {
	return fmt.Sprintf("(%s)", ex.inner)
}
func (ex *Parentheses) Children() []Expression {
	return []Expression{ex.inner}
}

type Ordering struct {
	Asc, NullsFirst bool // ARCH: consider *bool for better stringers (and better roundtrip tests)
	inner           Expression
}

func (ex *Ordering) ReturnType(ts column.TableSchema) (column.Schema, error) {
	return ex.inner.ReturnType(ts)
}
func (ex *Ordering) String() string {
	asc, nullsFirst := "ASC", "NULLS FIRST"
	if !ex.Asc {
		asc = "DESC"
	}
	if !ex.NullsFirst {
		nullsFirst = "NULLS LAST"
	}
	return fmt.Sprintf("%s %s %s", ex.inner, asc, nullsFirst)
}
func (ex *Ordering) Children() []Expression {
	return []Expression{ex.inner}
}
