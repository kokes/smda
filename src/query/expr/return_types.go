package expr

import (
	"fmt"

	"github.com/kokes/smda/src/database"
)

func (expr *Expression) IsValid(ts database.TableSchema) bool {
	return true // TODO
}

// TODO: will we define the name? As some sort of a composite of the actions taken?
// does this even need to return errors? If we always call IsValid outside of this, then this will
// always return a type
func (expr *Expression) ReturnType(ts database.TableSchema) (database.ColumnSchema, error) {
	// if !expr.IsValid { return 0, errors.New...}
	schema := database.ColumnSchema{}
	switch expr.etype {
	case exprLiteralInt:
		schema.Dtype = database.DtypeInt
		schema.Nullable = false
	case exprLiteralFloat:
		schema.Dtype = database.DtypeFloat
		schema.Nullable = false
	// case exprLiteralBool: // the parser does not support this yet
	// 	schema.Dtype = database.DtypeBool
	// 	schema.Nullable = false
	case exprLiteralString:
		schema.Dtype = database.DtypeString
		schema.Nullable = false

	// TODO:
	// case exprFunCall:
	case exprIdentifier:
		_, col, err := ts.LocateColumn(expr.value)
		if err != nil {
			return schema, err
		}
		schema = database.ColumnSchema{
			Name:     col.Name,
			Dtype:    col.Dtype,
			Nullable: col.Nullable,
		}

	case exprEquality, exprNequality, exprGreaterThan, exprGreaterThanEqual, exprLessThan, exprLessThanEqual:
		schema.Dtype = database.DtypeBool
		// schema.Nullable = ... // depends on the children's nullability
	case exprAddition, exprSubtraction, exprDivision, exprMultiplication:
		c1, err := expr.children[0].ReturnType(ts)
		if err != nil {
			return schema, err
		}
		c2, err := expr.children[1].ReturnType(ts)
		if err != nil {
			return schema, err
		}
		schema.Dtype = database.DtypeFloat
		// int operations (apart from division) will result back in ints
		if expr.etype != exprDivision && (c1.Dtype == database.DtypeInt && c2.Dtype == database.DtypeInt) {
			schema.Dtype = database.DtypeInt
		}
		schema.Nullable = c1.Nullable || c2.Nullable
	default:
		return schema, fmt.Errorf("TODO: %v", expr.etype)
	}
	return schema, nil
}
