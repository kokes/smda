package query

import (
	"errors"
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
	Select    []*expr.Expression `json:"select,omitempty"`
	Dataset   database.UID       `json:"dataset"`
	Filter    *expr.Expression   `json:"filter,omitempty"`
	Aggregate []string           `json:"aggregate"`
	Limit     *int               `json:"limit,omitempty"`
}

// OPTIM: this filters the whole dataset, but it we may only need to filter a single stripe - e.g. if we have no order or
// groupby clause and a limit (implicit or explicit)
func filter(db *database.Database, ds *database.Dataset, filterExpr *expr.Expression) ([]*bitmap.Bitmap, error) {
	rettype, err := filterExpr.ReturnType(ds.Schema)
	if err != nil {
		return nil, err
	}
	if rettype.Dtype != column.DtypeBool {
		return nil, fmt.Errorf("can only filter by expressions that return booleans, got %v that returns %v", filterExpr, rettype.Dtype)
	}
	var retval []*bitmap.Bitmap
	colnames := filterExpr.ColumnsUsed()
	for _, stripe := range ds.Stripes {
		columns, err := db.ReadColumnsFromStripeByNames(ds, stripe, colnames)
		if err != nil {
			return nil, err
		}
		fvals, err := expr.Evaluate(filterExpr, colnames, columns)
		if err != nil {
			return nil, err
		}
		// it's essential that we clone the bool column here (implicitly in Truths),
		// because this bitmap may be truncated later on (e.g. in KeepFirstN)
		// and expr.Evaluate may return a reference, not a clone (e.g. in exprIdent)
		bm := fvals.(*column.ChunkBools).Truths()
		retval = append(retval, bm)
	}

	return retval, nil
}

// TODO: this is not expression aware, also ignores q.Select
func aggregate(db *database.Database, ds *database.Dataset, exprs []string) ([]column.Chunk, error) {
	if len(exprs) == 0 {
		return nil, errors.New("cannot aggregate by an empty clause, need at least one expression")
	}

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

		// TODO: we don't have a stripe length property - should we?
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

// Result holds the result of a query, at this point it's fairly literal - in the future we may want
// a Result to be a Dataset of its own (for better interoperability, persistence, caching etc.)
type Result struct {
	Columns []string       `json:"columns"`
	Data    []column.Chunk `json:"data"`
}

// Run runs a given query against this database
// TODO: we have to differentiate between input errors and runtime errors (errors.Is?)
// the former should result in a 4xx, the latter in a 5xx
func Run(db *database.Database, q Query) (*Result, error) {
	res := &Result{
		Columns: make([]string, 0),
		Data:    make([]column.Chunk, 0),
	}

	ds, err := db.GetDataset(q.Dataset)
	if err != nil {
		return nil, err
	}

	var bms []*bitmap.Bitmap
	if q.Filter != nil {
		bms, err = filter(db, ds, q.Filter)
		if err != nil {
			return nil, err
		}
	}

	if q.Aggregate != nil {
		columns, err := aggregate(db, ds, q.Aggregate)
		if err != nil {
			return nil, err
		}
		res.Columns = q.Aggregate // no projections yet
		res.Data = columns
		return res, nil
		// no limit?
	}

	// this is a branch for non-aggregate queries
	for _, col := range q.Select {
		res.Columns = append(res.Columns, col.String())
		rschema, err := col.ReturnType(ds.Schema)
		if err != nil {
			return nil, err
		}
		res.Data = append(res.Data, column.NewChunkFromSchema(rschema))
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
		for j, colExpr := range q.Select {
			colnames := colExpr.ColumnsUsed() // OPTIM: calculated for each stripe, can be cached
			columns, err := db.ReadColumnsFromStripeByNames(ds, stripeID, colnames)
			if err != nil {
				return nil, err
			}
			col, err := expr.Evaluate(colExpr, colnames, columns)
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
