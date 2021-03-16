package query

import (
	"errors"
	"fmt"

	"github.com/kokes/smda/src/bitmap"
	"github.com/kokes/smda/src/column"
	"github.com/kokes/smda/src/database"
	"github.com/kokes/smda/src/query/expr"
)

var errNoProjection = errors.New("no expressions specified to be selected")
var errInvalidLimitValue = errors.New("invalid limit value")
var errInvalidProjectionInAggregation = errors.New("selections in aggregating expressions need to be either the group by clauses or aggregating expressions (e.g. sum(foo))")

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
	// TODO: PAFilter (post-aggregation filter, == having) - check how it behaves without aggregations elsewhere
}

// Result holds the result of a query, at this point it's fairly literal - in the future we may want
// a Result to be a Dataset of its own (for better interoperability, persistence, caching etc.)
type Result struct {
	Schema database.TableSchema `json:"schema"`
	Data   []column.Chunk       `json:"data"`
}

func filterStripe(db *database.Database, ds *database.Dataset, stripe database.Stripe, filterExpr *expr.Expression, colData map[string]column.Chunk) (*bitmap.Bitmap, error) {
	fvals, err := expr.Evaluate(filterExpr, stripe.Length, colData, nil)
	if err != nil {
		return nil, err
	}
	// it's essential that we clone the bool column here (implicitly in Truths),
	// because this bitmap may be truncated later on (e.g. in KeepFirstN)
	// and expr.Evaluate may return a reference, not a clone (e.g. in exprIdent)
	bm := fvals.(*column.ChunkBools).Truths()
	return bm, nil
}

// ARCH/OPTIM: there are a few issues here:
// 1) we don't cache the string values anywhere, so this is potentially expensive
// 2) we walk the slice instead of building a map once (essentially the same point)
// 3) we use .String() instead of .value - but will .value work if a projection
//    is `a+b` and the groupby expression is `A + B`? (test all this)
func lookupExpr(needle *expr.Expression, haystack []*expr.Expression) int {
	ns := needle.String()
	for j, ex := range haystack {
		if ex.String() == ns {
			return j
		}
	}
	return -1
}

// OPTIM: here are some rough calculations from running timers in the stripe loop (the only expensive part)
// loading data from disk: 133ms, hashing: 55ms, prune bitmaps prep: 23ms, updating aggregators: 28ms
// everything else is way faster
// OPTIM: if there's GROUPBY+LIMIT (and without ORDERBY), we can shortcircuit the hashing part - once we
// reach ndistinct == LIMIT, we can stop
func aggregate(db *database.Database, ds *database.Dataset, groupbys []*expr.Expression, projs []*expr.Expression, filterExpr *expr.Expression) ([]column.Chunk, error) {
	// we need to validate all projections - they either need to be in the groupby clause
	// or be aggregating (e.g. sum(ints) -> int)
	// we'll also collect all the aggregating expressions, so that we can feed them individual chunks
	// ARCH/TODO: move this to Run(), we need to do some err checking here, two cases come to mind:
	// 1. if someone passes a query `select: foo, sum(baz)`, we must tell them early on it doesn't make sense,
	//    now it redirects to a plain select and says `sum` doesn't exist as a projection
	// 2. if someone passes a plain `sum(foo)` with no aggregation, we want them to end up here
	//    (it's monkeypatched for now via `allAggregations`)
	var aggexprs []*expr.Expression
	for _, proj := range projs {
		aggexpr, err := expr.AggExpr(proj)
		if err != nil {
			return nil, err
		}
		if aggexpr != nil {
			aggexprs = append(aggexprs, aggexpr...)
			continue
		}
		pos := lookupExpr(proj, groupbys)
		if pos == -1 {
			return nil, fmt.Errorf("%w: %v", errInvalidProjectionInAggregation, proj)
		}
	}
	for _, aggexpr := range aggexprs {
		if err := aggexpr.InitAggregator(ds.Schema); err != nil {
			return nil, err
		}
	}

	var columnNames []string
	if filterExpr != nil {
		columnNames = expr.ColumnsUsed(ds.Schema, append(groupbys, append(projs, filterExpr)...)...)
	} else {
		columnNames = expr.ColumnsUsed(ds.Schema, append(groupbys, projs...)...)
	}
	groups := make(map[uint64]uint64)
	// ARCH: `nrc` and `rcs` are not very descriptive
	nrc := make([]column.Chunk, len(groupbys))
	for _, stripe := range ds.Stripes {
		stripeLength := stripe.Length
		var filter *bitmap.Bitmap
		rcs := make([]column.Chunk, len(groupbys))
		columnData, err := db.ReadColumnsFromStripeByNames(ds, stripe, columnNames)
		if err != nil {
			return nil, err
		}
		if filterExpr != nil {
			filter, err = filterStripe(db, ds, stripe, filterExpr, columnData)
			if err != nil {
				return nil, err
			}
			stripeLength = filter.Count()
		}

		// 1) evaluate all the aggregation expressions (those expressions that determine groups, e.g. `country`)
		for j, expression := range groupbys {
			rc, err := expr.Evaluate(expression, stripeLength, columnData, filter)
			if err != nil {
				return nil, err
			}
			rcs[j] = rc
		}
		hashes := make([]uint64, stripeLength) // preserves unique rows (their hashes); OPTIM: preallocate some place
		bm := bitmap.NewBitmap(stripeLength)   // denotes which rows are the unique ones
		for j, rc := range rcs {
			rc.Hash(j, hashes)
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
			if err := expr.UpdateAggregator(aggexpr, hashes, len(groups), columnData, filter); err != nil {
				return nil, err
			}
		}
	}
	// 3) resolve aggregating expressions
	ret := make([]column.Chunk, len(projs))
	for j, gr := range groupbys {
		// OPTIM: we did this once already
		pos := lookupExpr(gr, projs)
		if pos == -1 {
			continue
		}
		ret[pos] = nrc[j]
	}
	for j, proj := range projs {
		if ret[j] != nil {
			// this is an aggregating expression, skip it
			continue
		}
		// we can pass in a nil map, because agg exprs get evaluated first
		// we can also pass in negative length, because it doesn't matter for resolvers
		// ARCH: but should we get the chunk length and pass it in, for good measure?
		// TODO/ARCH: shouldn't this call Resolve directly (if we exporter the aggregator)? It's kind
		// of funky to hide the Resolver under Evaluate
		agg, err := expr.Evaluate(proj, -1, nil, nil)
		if err != nil {
			return nil, err
		}
		ret[j] = agg
	}

	return ret, nil
}

// Run runs a given query against this database
// TODO: we have to differentiate between input errors and runtime errors (errors.Is?)
// the former should result in a 4xx, the latter in a 5xx
func Run(db *database.Database, q Query) (*Result, error) {
	if len(q.Select) == 0 {
		return nil, errNoProjection
	}
	res := &Result{
		Schema: make([]column.Schema, 0, len(q.Select)),
		Data:   make([]column.Chunk, 0),
	}

	ds, err := db.GetDataset(q.Dataset)
	if err != nil {
		return nil, err
	}

	allAggregations := true
	for _, col := range q.Select {
		rschema, err := col.ReturnType(ds.Schema)
		if err != nil {
			return nil, err
		}
		res.Schema = append(res.Schema, rschema)
		// ARCH: this won't be used in aggregation, is that okay?
		res.Data = append(res.Data, column.NewChunkFromSchema(rschema))

		aggexpr, err := expr.AggExpr(col)
		if err != nil {
			return nil, err
		}
		if aggexpr == nil {
			allAggregations = false
		}
	}

	if q.Filter != nil {
		rettype, err := q.Filter.ReturnType(ds.Schema)
		if err != nil {
			return nil, err
		}
		if rettype.Dtype != column.DtypeBool {
			return nil, fmt.Errorf("can only filter by expressions that return booleans, got %v that returns %v", q.Filter, rettype.Dtype)
		}
	}

	if q.Aggregate != nil || allAggregations {
		columns, err := aggregate(db, ds, q.Aggregate, q.Select, q.Filter)
		if err != nil {
			return nil, err
		}
		res.Data = columns
		return res, nil
		// no limit?
	}

	limit := -1
	if q.Limit != nil {
		if *q.Limit < 0 {
			return nil, fmt.Errorf("%w: %v", errInvalidLimitValue, *q.Limit)
		}
		limit = *q.Limit
	}
	for _, stripe := range ds.Stripes {
		var colnames []string
		if q.Filter == nil {
			colnames = expr.ColumnsUsed(ds.Schema, q.Select...)
		} else {
			colnames = expr.ColumnsUsed(ds.Schema, append(q.Select, q.Filter)...)
		}
		columns, err := db.ReadColumnsFromStripeByNames(ds, stripe, colnames)
		if err != nil {
			return nil, err
		}
		var filter *bitmap.Bitmap
		loadFromStripe := stripe.Length
		if q.Filter != nil {
			filter, err = filterStripe(db, ds, stripe, q.Filter, columns)
			if err != nil {
				return nil, err
			}
			if limit >= 0 && filter.Count() > limit {
				filter.KeepFirstN(limit)
			}
		} else {
			// TODO/ARCH: all this limit handling is a bit clunky, simplify it quite a bit
			if limit >= 0 && stripe.Length > limit {
				filter = bitmap.NewBitmap(stripe.Length)
				filter.Invert()
				filter.KeepFirstN(limit)
			}
		}
		if filter != nil && filter.Count() < loadFromStripe {
			loadFromStripe = filter.Count()
		}
		if limit >= 0 && limit < loadFromStripe {
			loadFromStripe = limit
		}
		if loadFromStripe == 0 {
			continue
		}
		limit -= loadFromStripe
		for j, colExpr := range q.Select {
			col, err := expr.Evaluate(colExpr, loadFromStripe, columns, filter)
			if err != nil {
				return nil, err
			}

			if err := res.Data[j].Append(col); err != nil {
				return nil, err
			}
		}
		if limit <= 0 {
			break
		}
	}

	return res, nil
}
