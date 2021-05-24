package expr

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/kokes/smda/src/column"
	"github.com/kokes/smda/src/database"
)

var errNoNestedAggregations = errors.New("cannot nest aggregations (e.g. sum(min(a)))")
var errTypeMismatch = errors.New("expecting compatible types")
var errNoTypes = errors.New("expecting at least one column")

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
	children := expr.Children()
	for _, ch := range children {
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
