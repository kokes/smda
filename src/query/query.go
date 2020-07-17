package query

import (
	"fmt"

	"github.com/kokes/smda/src/bitmap"
	"github.com/kokes/smda/src/column"
	"github.com/kokes/smda/src/database"
	"github.com/kokes/smda/src/query/expr"
)

// Query describes what we want to retrieve from a given dataset
// There are basically four places you need to edit (and test!) in order to extend this:
// 1) The engine itself needs to support this functionality (usually a method on Dataset or column.Chunk)
// 2) The query method has to be able to translate query parameters to the engine
// 3) The query endpoint handler needs to be able to process the incoming body
//    to the Query struct (the Unmarshaler should mostly take care of this)
// 4) The HTML/JS frontend needs to incorporate this in some way
type Query struct {
	Dataset   database.UID     `json:"dataset"`
	Filter    *expr.Expression `json:"filter,omitempty"`
	Aggregate []string         `json:"aggregate"` // this will be *Expr at some point
	Limit     *int             `json:"limit,omitempty"`
}

// TODO: to be implemented (needs eval)
// will we need three-valued logic here? Or will be simply default null to false? because that's how
// the where clause behaves
func Filter(db *database.Database, ds *database.Dataset, fe *expr.Expression) ([]*bitmap.Bitmap, error) {
	// new implementation draft:
	rettype, err := fe.ReturnType(ds.Schema)
	if err != nil {
		return nil, err
	}
	if rettype.Dtype != column.DtypeBool {
		return nil, fmt.Errorf("can only filter by expressions that return booleans, got %v that returns %v", fe, rettype.Dtype)
	}
	colnames := fe.ColumnsUsed()
	for _, stripe := range ds.Stripes {
		columns, err := db.ReadColumnsFromStripeByNames(ds, stripe, colnames)
		if err != nil {
			return nil, err
		}
		_ = columns
		// eval(fe, colnames, columns) -> (TypedColumn[columnBool], error)
	}

	// old implementation:
	// colIndex, _, err := ds.Schema.LocateColumn(fe.Column)
	// if err != nil {
	// 	return nil, err
	// }
	// bms := make([]*bitmap.Bitmap, 0, len(ds.Stripes))
	// for _, stripe := range ds.Stripes {
	// 	col, err := db.ReadColumnFromStripe(ds, stripe, colIndex)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	// TODO: the thing with column.Chunk.Filter not returning an error is that it panic
	// 	// when using a non-supported operator - this does not lead to good user experience, plus
	// 	// it allows the user to crash the system without a great logging experience
	// 	bm := col.Filter(fe.Operator, fe.Argument)
	// 	bms = append(bms, bm)
	// }
	return nil, nil
}

func Aggregate(db *database.Database, ds *database.Dataset, exprs []string) ([]column.Chunk, error) {
	// TODO: fail if len(exprs) == 0? it will panic later anyway

	nrc := make([]column.Chunk, 0, len(exprs))
	colIndices := make([]int, 0, len(exprs))

	for _, expr := range exprs {
		idx, col, err := ds.Schema.LocateColumn(expr)
		if err != nil {
			return nil, err
		}
		nrc = append(nrc, column.NewChunkFromSchema(col))
		colIndices = append(colIndices, idx)
	}

	groups := make(map[uint64]int)
	for _, stripeID := range ds.Stripes {
		rcs := make([]column.Chunk, 0, len(exprs))
		for _, colIndex := range colIndices {
			rc, err := db.ReadColumnFromStripe(ds, stripeID, colIndex)
			if err != nil {
				return nil, err
			}
			rcs = append(rcs, rc)
		}

		// we don't have a stripe length property - should we?
		ln := rcs[0].Len()
		hashes := make([]uint64, ln) // preserves unique rows (their hashes)
		bm := bitmap.NewBitmap(ln)   // denotes which rows are the unique ones
		for _, rc := range rcs {
			rc.Hash(hashes)
		}
		for row, hash := range hashes {
			if _, ok := groups[hash]; !ok {
				groups[hash] = len(groups)
				// it's a new value, set our bitmap, so that we can prune it later
				bm.Set(row, true)
			}
		}

		// we have identified new rows in our stripe, add it to our existing columns
		for j, rc := range rcs {
			if err := nrc[j].Append(rc.Prune(bm)); err != nil {
				return nil, err
			}
		}
		// also add some meta slice of "group ID" and return it or incorporate it in the []column.Chunk
	}

	return nrc, nil
}

// QueryResult holds the result of a query, at this point it's fairly literal - in the future we may want
// a QueryResult to be a Dataset of its own (for better interoperability, persistence, caching etc.)
type QueryResult struct {
	Columns []string       `json:"columns"`
	Data    []column.Chunk `json:"data"`
}

// TODO: we have to differentiate between input errors and runtime errors (errors.Is?)
// the former should result in a 4xx, the latter in a 5xx
func QueryData(db *database.Database, q Query) (*QueryResult, error) {
	res := &QueryResult{
		Columns: make([]string, 0),
		Data:    make([]column.Chunk, 0),
	}

	ds, err := db.GetDataset(q.Dataset)
	if err != nil {
		return nil, err
	}

	var bms []*bitmap.Bitmap
	if q.Filter != nil {
		bms, err = Filter(db, ds, q.Filter)
		if err != nil {
			return nil, err
		}
	}

	if q.Aggregate != nil {
		columns, err := Aggregate(db, ds, q.Aggregate)
		if err != nil {
			return nil, err
		}
		res.Columns = q.Aggregate // no projections yet
		res.Data = columns
		return res, nil
		// no limit?
	}

	// this is a branch for non-aggregate queries

	for _, col := range ds.Schema {
		res.Columns = append(res.Columns, col.Name)
		res.Data = append(res.Data, column.NewChunkFromSchema(col))
	}

	limit := -1
	if q.Limit != nil {
		if *q.Limit < 0 {
			return nil, fmt.Errorf("invalid limit value: %v", *q.Limit)
		}
		limit = *q.Limit
	}
	for stripeIndex, stripeID := range ds.Stripes {
		// if no relevant data in this stripe, skip it
		if bms != nil {
			filteredLen := bms[stripeIndex].Count()
			if filteredLen == 0 {
				continue
			}
			if limit >= 0 && filteredLen > limit {
				bms[stripeIndex].KeepFirstN(limit)
			}
			limit -= filteredLen
		}
		var bmnf *bitmap.Bitmap // bitmap for non-filtered data - I really dislike the way this is handled (TODO)
		for j := range ds.Schema {
			col, err := db.ReadColumnFromStripe(ds, stripeID, j)
			if err != nil {
				return nil, err
			}
			// prune when filtering
			if bms != nil {
				col = col.Prune(bms[stripeIndex])
			}
			// prune non-filtered stripes as well (when limit is applied)
			// for each stripe, set up the bitmap when in the first column (because we don't
			// know the columns' length before that)
			if limit >= 0 && j == 0 && bms == nil {
				ln := col.Len()
				if ln <= limit {
					limit -= ln
				} else {
					bmnf = bitmap.NewBitmap(ln)
					bmnf.Invert()
					bmnf.KeepFirstN(limit)
				}
			}
			if bmnf != nil {
				col = col.Prune(bmnf)
			}
			if err := res.Data[j].Append(col); err != nil {
				return nil, err
			}
		}
		if bmnf != nil {
			limit -= bmnf.Count()
			bmnf = nil
		}
		if limit <= 0 {
			break
		}
	}

	return res, nil
}
