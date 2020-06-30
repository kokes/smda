package expr

import (
	"fmt"

	"github.com/kokes/smda/src/database"
)

func (expr *Expression) IsValid(ts database.TableSchema) bool {
	return true // TODO
}

// TODO: will we define the name? As some sort of a composite of the actions taken?
func (expr *Expression) ReturnType(ts database.TableSchema) (database.ColumnSchema, error) {
	// if !expr.IsValid { return 0, errors.New...}
	schema := database.ColumnSchema{}
	switch expr.etype {
	case exprLiteralInt:
		schema.Dtype = database.DtypeInt
		schema.Nullable = false
	case exprEquality, exprNequality, exprGreaterThan, exprGreaterThanEqual, exprLessThan, exprLessThanEqual:
		schema.Dtype = database.DtypeBool
		// schema.Nullable = ... // depends on the children's nullability
	default:
		panic(fmt.Sprintf("TODO: %v", expr.etype))
	}
	return schema, nil
}
