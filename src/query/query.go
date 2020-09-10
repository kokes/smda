package query

import (
	"errors"
	"fmt"

	"github.com/kokes/smda/src/bitmap"
	"github.com/kokes/smda/src/column"
	"github.com/kokes/smda/src/database"
	"github.com/kokes/smda/src/query/expr"
)

var errInvalidLimitValue = errors.New("invalid limit value")

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
	Aggregate []*expr.Expression `json:"aggregate,omitempty"`
	Limit     *int               `json:"limit,omitempty"`
}

func colmap(columns []string, coldata []column.Chunk) map[string]column.Chunk {
	colmap := make(map[string]column.Chunk)
	for j, colname := range columns {
		colmap[colname] = coldata[j]
	}
	return colmap
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
		fvals, err := expr.Evaluate(filterExpr, colmap(colnames, columns))
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

// doesn't seem to be NULL-aware
// also, downstream .Hash methods do not take column order into consideration (XOR)
func aggregate(db *database.Database, ds *database.Dataset, groupbys []*expr.Expression, projs []*expr.Expression) ([]column.Chunk, error) {
	if len(groupbys) == 0 {
		return nil, errors.New("cannot aggregate by an empty clause, need at least one expression")
	}

	// these are subexpressions from our projections
	// e.g. 2*sum(foo+bar) turns into sum(foo+bar)
	// we need the aggregating expressions isolated, so that we can
	// feed them individual chunks
	var aggexprs []*expr.Expression
	for _, g := range projs {
		aggexpr := expr.AggExpr(g)
		if aggexpr != nil {
			aggexprs = append(aggexprs, aggexpr...)
		}
	}

	columnNames := expr.ColumnsUsed(groupbys...)
	// TODO: there will be duplicates here
	columnNames = append(columnNames, expr.ColumnsUsed(projs...)...)
	groups := make(map[uint64]uint64)
	nrc := make([]column.Chunk, len(groupbys)) // this will eventually be len(projs)
	for _, stripeID := range ds.Stripes {
		rcs := make([]column.Chunk, len(groupbys))
		columnData, err := db.ReadColumnsFromStripeByNames(ds, stripeID, columnNames)
		if err != nil {
			return nil, err
		}
		columnMap := colmap(columnNames, columnData) // consider changing ReadColumnsFromStripeByNames to return this map

		// 1) evaluate all the aggregation expressions (those expressions that determine groups, e.g. `country`)
		for j, expression := range groupbys {
			rc, err := expr.Evaluate(expression, columnMap)
			if err != nil {
				return nil, err
			}
			rcs[j] = rc
		}

		// TODO: we don't have a stripe length property - should we?
		ln := rcs[0].Len()
		hashes := make([]uint64, ln) // preserves unique rows (their hashes); OPTIM: preallocate some place
		bm := bitmap.NewBitmap(ln)   // denotes which rows are the unique ones
		for _, rc := range rcs {
			rc.Hash(hashes)
		}
		for row, hash := range hashes {
			if _, ok := groups[hash]; !ok {
				groups[hash] = uint64(len(groups))
				// it's a new value, set our bitmap, so that we can prune it later
				bm.Set(row, true)
			}
		}

		// we have identified new rows in our stripe, add it to our existing columns
		for j, rc := range rcs {
			if nrc[j] == nil {
				nrc[j] = rc.Prune(bm)
				continue
			}
			if err := nrc[j].Append(rc.Prune(bm)); err != nil {
				return nil, err
			}
		}

		// 2) update our aggregating expressions (e.g. `sum(a)`)
		// we no longer need the `hashes` for this stripe, so we'll repurpose it
		// to get information on groups (buckets)
		for j, el := range hashes {
			hashes[j] = groups[el]
		}
		for _, aggexpr := range aggexprs {
			if err := expr.UpdateAggregator(aggexpr, hashes, len(groups), columnMap); err != nil {
				return nil, err
			}
		}
	}
	// 3) resolve aggregating expressions

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
		columns, err := aggregate(db, ds, q.Aggregate, q.Select)
		if err != nil {
			return nil, err
		}
		// no projections yet
		res.Columns = make([]string, 0, len(columns))
		for _, col := range q.Aggregate {
			res.Columns = append(res.Columns, col.String())
		}
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
			return nil, fmt.Errorf("%w: %v", errInvalidLimitValue, *q.Limit)
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

			col, err := expr.Evaluate(colExpr, colmap(colnames, columns))
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
