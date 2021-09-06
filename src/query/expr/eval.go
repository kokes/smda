package expr

import (
	"errors"
	"fmt"
	"strings"

	"github.com/kokes/smda/src/bitmap"
	"github.com/kokes/smda/src/column"
)

var errQueryPatternNotSupported = errors.New("query pattern not supported")
var errFunctionNotImplemented = errors.New("function not implemented")
var errDivisionByZero = errors.New("division by zero") // TODO/ARCH: hint that we can use NULLIF?

// OPTIM: we're doing a lot of type shenanigans at runtime - when we evaluate a function on each stripe, we do
// the same tree of operations - this applies not just here, but in projections.go as well - e.g. we know that
// if we have `intA - intB`, we'll run a function for ints - we don't need to decide that for each stripe
func Evaluate(expr Expression, chunkLength int, columnData map[string]column.Chunk, filter *bitmap.Bitmap) (column.Chunk, error) {
	// TODO: test this via UpdateAggregator
	if f, ok := expr.(*Function); ok && f.aggregator != nil {
		// TODO: assert that filters !== nil?
		return f.aggregator.Resolve()
	}

	switch node := expr.(type) {
	case *Parentheses:
		return Evaluate(node.inner, chunkLength, columnData, filter)
	case *Prefix:
		switch node.operator {
		case tokenNot:
			inner, err := Evaluate(node.right, chunkLength, columnData, filter)
			if err != nil {
				return nil, err
			}
			return column.EvalNot(inner)
		case tokenAdd:
			// noop
			return Evaluate(node.right, chunkLength, columnData, filter)
		case tokenSub:
			// OPTIM: this whole block will benefit from constant folding, especially if the child is a literal int/float
			newExpr := &Infix{
				operator: tokenMul,
				left:     &Integer{value: -1},
				right:    node.right,
			}
			return Evaluate(newExpr, chunkLength, columnData, filter)
		default:
			return nil, fmt.Errorf("unknown prefix token: %v", node.operator)
		}
	case *Identifier:
		lookupValue := node.Name
		if !node.quoted {
			lookupValue = strings.ToLower(lookupValue)
		}
		col, ok := columnData[lookupValue]
		if !ok {
			// we validated the expression, so this should not happen?
			// perhaps to catch bugs in case folding?
			return nil, fmt.Errorf("column %v not found", node.Name)
		}
		if filter != nil {
			return col.Prune(filter), nil
		}
		return col, nil
	// since these literals don't interact with any "dense" column chunks, we need
	// to pass in their lengths
	case *Integer:
		return column.NewChunkLiteralInts(node.value, chunkLength), nil
	case *Float:
		return column.NewChunkLiteralFloats(node.value, chunkLength), nil
	case *Bool:
		return column.NewChunkLiteralBools(node.value, chunkLength), nil
	case *String:
		return column.NewChunkLiteralStrings(node.value, chunkLength), nil
	case *Null:
		return column.NewChunkLiteralTyped("", column.DtypeNull, chunkLength)
	case *Function:
		// OPTIM: we could optimise shallow function calls - e.g. `log(foo) > 1` doesn't need
		// `log(foo)` as a newly allocated chunk, we can compute that on the fly
		if node.evaler == nil {
			return nil, fmt.Errorf("%w: %s", errFunctionNotImplemented, node.name)
		}
		// ARCH: abstract out this `children` construction and use it elsewhere (in exprEquality etc.)
		children := make([]column.Chunk, 0, len(node.Children()))
		for _, ch := range node.Children() {
			child, err := Evaluate(ch, chunkLength, columnData, filter)
			if err != nil {
				return nil, err
			}
			children = append(children, child)
		}
		return node.evaler(children...)
	case *Relabel:
		return Evaluate(node.inner, chunkLength, columnData, filter)
	case *Infix:
		c1, err := Evaluate(node.left, chunkLength, columnData, filter)
		if err != nil {
			return nil, err
		}
		c2, err := Evaluate(node.right, chunkLength, columnData, filter)
		if err != nil {
			return nil, err
		}

		// TODO(next): test null=null, null>null (in filters, groupbys, selects, wherever)
		// we have tested this in SELECTs, the rest needs to be tested in query_test.go
		if c1.Dtype() == column.DtypeNull && c2.Dtype() == column.DtypeNull {
			return nil, errQueryPatternNotSupported // ARCH: wrap?
		}

		// ARCH: what should `2-NULL` be in terms of types? Is this dtypenull or dtypeint [with all nulls]? Check pg or other engines
		// ARCH: this might fit better in a isNull/isNotNull function, which we might either introduce or we could
		// rewrite all `x=null` expressions into it during our AST shenanigans
		// even though we don't support this evaluation, we need to honor the input types
		if c1.Dtype() == column.DtypeNull || c2.Dtype() == column.DtypeNull {
			if !(node.operator == tokenEq || node.operator == tokenNeq || node.operator == tokenIs) {
				if node.operator == tokenAdd || node.operator == tokenSub || node.operator == tokenMul || node.operator == tokenQuo {
					nulls := bitmap.NewBitmap(chunkLength)
					nulls.Invert()
					// ARCH: duplicating logic from ReturnTypes
					if c1.Dtype() == column.DtypeFloat || c2.Dtype() == column.DtypeFloat || node.operator == tokenQuo {
						return column.NewChunkFloatsFromSlice(make([]float64, chunkLength), nulls), nil
					}
					return column.NewChunkIntsFromSlice(make([]int64, chunkLength), nulls), nil
				} else {
					// we need to return a boolean chunk, but filled with all nulls
					ch := column.NewChunkBoolsFromBitmap(bitmap.NewBitmap(chunkLength))
					ch.Nullability = bitmap.NewBitmap(chunkLength)
					ch.Nullability.Invert()
					return ch, nil
				}
			}

			// there are now three cases to consider for equality/nequality
			// 1) the non-null column is a literal - we can easily return a bool literal bool a result (since literals cannot be nullable)
			// 2) the non-null column is not nullable - we can return the same literal bool as above
			// 3) the non-null column is nullable, we have to take its nullability vector and create a new chunk from it
			// TODO(next): test all cases thoroughly
			cdata := c1
			if c1.Dtype() == column.DtypeNull {
				cdata = c2
			}
			nb := cdata.Base().Nullability

			// literals cannot be null, so a comparison to nulls is simple
			// this should really be done in constant folding
			if cdata.Base().IsLiteral || nb == nil {
				if node.operator == tokenEq || node.operator == tokenIs {
					return column.NewChunkLiteralTyped("false", column.DtypeBool, chunkLength)
				} else if node.operator == tokenNeq {
					return column.NewChunkLiteralTyped("true", column.DtypeBool, chunkLength)
				} else {
					panic("unreachable")
				}
			}

			if node.operator == tokenEq || node.operator == tokenIs {
				return column.NewChunkBoolsFromBitmap(nb.Clone()), nil
			} else if node.operator == tokenNeq {
				values := nb.Clone()
				values.Invert()
				return column.NewChunkBoolsFromBitmap(values), nil
			} else {
				panic("unreachable")
			}
		}

		switch node.operator {
		case tokenAnd:
			return column.EvalAnd(c1, c2)
		case tokenOr:
			return column.EvalOr(c1, c2)
		case tokenEq, tokenIs:
			return column.EvalEq(c1, c2)
		case tokenNeq:
			return column.EvalNeq(c1, c2)
		case tokenLt:
			return column.EvalLt(c1, c2)
		case tokenLte:
			return column.EvalLte(c1, c2)
		case tokenGt:
			return column.EvalGt(c1, c2)
		case tokenGte:
			return column.EvalGte(c1, c2)
		case tokenAdd:
			return column.EvalAdd(c1, c2)
		case tokenSub:
			return column.EvalSubtract(c1, c2)
		case tokenQuo:
			div, err := column.EvalDivide(c1, c2)
			if err != nil {
				return nil, err
			}
			// investigate if `c2` contains zeros - if so, trigger errDivisionByZero (SQL standard)
			// OPTIM: it would probably be faster to iterate c2.data, but this is cleaner
			eq, err := column.EvalEq(c2, column.NewChunkLiteralFloats(0, c2.Len()))
			if err != nil {
				return nil, err
			}
			zeros := eq.(*column.ChunkBools).Truths()
			if zeros.Count() > 0 {
				return nil, errDivisionByZero
			}
			return div, nil
		case tokenMul:
			return column.EvalMultiply(c1, c2)
		default:
			return nil, fmt.Errorf("unknown infix token: %v", node.operator)
		}
	default:
		return nil, fmt.Errorf("expression %v not supported: %w", expr, errQueryPatternNotSupported)
	}
}

func UpdateAggregator(fun *Function, buckets []uint64, ndistinct int, columnData map[string]column.Chunk, filter *bitmap.Bitmap) error {
	// if expr.aggregator == nil {err}
	// if len(expr.children) != 1 {err}// what about count()?

	// e.g. sum(1+foo) needs `1+foo` evaluated first, then we feed the resulting
	// chunk to the sum aggregator
	var child column.Chunk
	var err error
	// in case we have e.g. `count()`, we cannot evaluate its children as there are none
	if len(fun.args) > 0 {
		child, err = Evaluate(fun.args[0], len(buckets), columnData, filter)
		if err != nil {
			return err
		}
	}
	fun.aggregator.AddChunk(buckets, ndistinct, child)
	return nil
}
