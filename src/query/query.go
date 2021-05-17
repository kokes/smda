package query

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/kokes/smda/src/bitmap"
	"github.com/kokes/smda/src/column"
	"github.com/kokes/smda/src/database"
	"github.com/kokes/smda/src/query/expr"
)

var errNoProjection = errors.New("no expressions specified to be selected")
var errInvalidLimitValue = errors.New("invalid limit value")
var errInvalidProjectionInAggregation = errors.New("selections in aggregating expressions need to be either the group by clauses or aggregating expressions (e.g. sum(foo))")

// Result holds the result of a query, at this point it's fairly literal - in the future we may want
// a Result to be a Dataset of its own (for better interoperability, persistence, caching etc.)
// ARCH/TODO: this is really a schema and `stripeData`, isn't it? Can we leverage that?
type Result struct {
	Schema column.TableSchema
	Length int
	Data   []column.Chunk

	// this is used for sorting
	rowIdxs    []int
	asc        []bool
	nullsfirst []bool
	// this does not allow for sorting by things not materialised by projections (ARCH?)
	sortColumnsIdxs []int
}

// Length might be much smaller than the data within (thanks to ORDER BY), so we should prune our columns
func (res *Result) Prune() {
	// bm := bitmap.NewBitmap(res.Length)

}

// TODO(next): test this
func (r *Result) MarshalJSON() ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	if _, err := buf.WriteString("{\n\t\"schema\": "); err != nil {
		return nil, err
	}
	if err := enc.Encode(r.Schema); err != nil {
		return nil, err
	}
	if _, err := buf.WriteString(fmt.Sprintf(",\n\"nrows\": %d", r.Length)); err != nil {
		return nil, err
	}

	// write data at last
	if _, err := buf.WriteString(",\n\"data\": ["); err != nil {
		return nil, err
	}

	for j := 0; j < r.Length; j++ {
		rownum := j
		if r.rowIdxs != nil {
			rownum = r.rowIdxs[j]
		}

		if err := buf.WriteByte('['); err != nil {
			return nil, err
		}
		for cn := 0; cn < len(r.Data); cn++ {
			if cn > 0 {
				if _, err := buf.WriteString(", "); err != nil {
					return nil, err
				}
			}
			// TODO(next)/OPTIM: literal optimisation - find out literals beforehand and pre-serialise them
			col := r.Data[cn]
			val, ok := col.JSONLiteral(rownum)
			if !ok {
				val = "null"
			}
			if _, err := buf.WriteString(val); err != nil {
				return nil, err
			}
		}
		if err := buf.WriteByte(']'); err != nil {
			return nil, err
		}
		if j < r.Length-1 {
			if _, err := buf.WriteString(",\n"); err != nil {
				return nil, err
			}
		}
	}

	if _, err := buf.WriteString("]"); err != nil {
		return nil, err
	}
	if _, err := buf.WriteString("\n}"); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
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
// TODO(next): this means we cannot compare projections with aggregations/orderings
// e.g. `select foo as bar order by foo` doesn't work, neither does `order by bar`
// relabeled projections just cannot be sorted on
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
func aggregate(db *database.Database, ds *database.Dataset, res *Result, q expr.Query) error {
	// we need to validate all projections - they either need to be in the groupby clause
	// or be aggregating (e.g. sum(ints) -> int)
	// we'll also collect all the aggregating expressions, so that we can feed them individual chunks
	// ARCH/TODO: move this to Run(), we need to do some err checking here, two cases come to mind:
	// 1. if someone passes a query `select: foo, sum(baz)`, we must tell them early on it doesn't make sense,
	//    now it redirects to a plain select and says `sum` doesn't exist as a projection
	// 2. if someone passes a plain `sum(foo)` with no aggregation, we want them to end up here
	//    (it's monkeypatched for now via `allAggregations`)
	var aggexprs []*expr.Expression
	for _, proj := range q.Select {
		aggexpr, err := expr.AggExpr(proj)
		if err != nil {
			return err
		}
		if aggexpr != nil {
			aggexprs = append(aggexprs, aggexpr...)
			continue
		}
		pos := lookupExpr(proj, q.Aggregate)
		if pos == -1 {
			return fmt.Errorf("%w: %v", errInvalidProjectionInAggregation, proj)
		}
	}
	for _, aggexpr := range aggexprs {
		if err := aggexpr.InitAggregator(ds.Schema); err != nil {
			return err
		}
	}

	columnNames := expr.ColumnsUsed(ds.Schema, append(q.Aggregate, q.Select...)...)
	if q.Filter != nil {
		// TODO(next): turns out we don't hit this branch at all in our tests!
		columnNames = append(columnNames, expr.ColumnsUsed(ds.Schema, q.Filter)...)
	}
	// TODO(next): load orderby columns? Do we need to? Should we allow loading more than is in projections/groupbys?
	// test what happens if we order by something not in select/groupby (we should check for it)
	groups := make(map[uint64]uint64)
	// ARCH: `nrc` and `rcs` are not very descriptive
	nrc := make([]column.Chunk, len(q.Aggregate))
	for _, stripe := range ds.Stripes {
		stripeLength := stripe.Length
		var filter *bitmap.Bitmap
		rcs := make([]column.Chunk, len(q.Aggregate))
		columnData, err := db.ReadColumnsFromStripeByNames(ds, stripe, columnNames)
		if err != nil {
			return err
		}
		if q.Filter != nil {
			filter, err = filterStripe(db, ds, stripe, q.Filter, columnData)
			if err != nil {
				return err
			}
			stripeLength = filter.Count()
		}

		// 1) evaluate all the aggregation expressions (those expressions that determine groups, e.g. `country`)
		for j, expression := range q.Aggregate {
			rc, err := expr.Evaluate(expression, stripeLength, columnData, filter)
			if err != nil {
				return err
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
				return err
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
				return err
			}
		}
	}
	// 3) resolve aggregating expressions
	ret := make([]column.Chunk, len(q.Select))
	for j, gr := range q.Aggregate {
		// OPTIM: we did this once already
		pos := lookupExpr(gr, q.Select)
		if pos == -1 {
			continue
		}
		ret[pos] = nrc[j]
	}
	for j, proj := range q.Select {
		if ret[j] != nil {
			// this is an aggregating expression, skip it
			continue
		}
		// we can pass in a nil map, because agg exprs get evaluated first
		// TODO/ARCH: shouldn't this call Resolve directly (if we exporter the aggregator)? It's kind
		// of funky to hide the Resolver under Evaluate
		agg, err := expr.Evaluate(proj, len(groups), nil, nil)
		if err != nil {
			return err
		}
		ret[j] = agg
	}

	res.Data = ret
	res.Length = ret[0].Len() // TODO(next): we can't have zero aggregations, right?

	if q.Order != nil {
		if err := reorder(res, q); err != nil {
			return err
		}
	}

	// OPTIM: if we push the limit somewhere above, we can simplify the aggregation itself
	//        we will still have to iterate all chunks, but the state will be smaller
	//        This will be an excellent optimisation for larger datasets - think queries
	//        like `select user_id, count() from activity group by user_id limit 100`
	// TODO(next): test this
	if q.Limit != nil && *q.Limit < res.Length {
		// we cannot prune the data as if it's ordered
		if q.Order == nil {
			bm := bitmap.NewBitmap(res.Length)
			bm.Invert()
			bm.KeepFirstN(*q.Limit)
			for j, col := range res.Data {
				res.Data[j] = col.Prune(bm)
			}
		}
		res.Length = *q.Limit
	}

	return nil
}

// ARCH: we might want to split this file up, it's getting a bit gnarly
func (res *Result) Len() int {
	return res.Length
}

func (res *Result) Swap(i, j int) {
	res.rowIdxs[i], res.rowIdxs[j] = res.rowIdxs[j], res.rowIdxs[i]
}

// based on the multi sorter in the sort Go docs
func (res *Result) Less(i, j int) bool {
	for pos, idx := range res.sortColumnsIdxs {
		// i, j don't signify the position in the chunk's data field, because we're mapping row ordering
		// using res.rowIdxs instead
		p1, p2 := res.rowIdxs[i], res.rowIdxs[j]
		cmp := res.Data[idx].Compare(res.asc[pos], res.nullsfirst[pos], p1, p2)
		if cmp == -1 {
			return true
		}
		if cmp == 1 {
			return false
		}
	}

	// all are equal, so use the last one - the docs say that... but by definition this must be false?
	// TODO(next): review this
	// return res.Data[idx].Compare(res.asc[pos], res.nullsfirst[pos], i, j) == -1
	return true // using true to get a stable sort?
}

func reorder(res *Result, q expr.Query) error {
	res.rowIdxs = make([]int, res.Length)
	for j := 0; j < res.Length; j++ {
		res.rowIdxs[j] = j
	}
	res.asc = make([]bool, len(q.Order))
	res.nullsfirst = make([]bool, len(q.Order))
	res.sortColumnsIdxs = make([]int, len(q.Order))
	for j := 0; j < len(q.Order); j++ {
		clause := q.Order[j]
		var asc, nullsFirst bool
		var needle *expr.Expression
		switch clause.Value() {
		case expr.SortAscNullsFirst:
			asc, nullsFirst = true, true
		case expr.SortAscNullsLast:
			asc, nullsFirst = true, false
		case expr.SortDescNullsFirst:
			asc, nullsFirst = false, true
		case expr.SortDescNullsLast:
			asc, nullsFirst = false, false
		default:
			// this means we didn't specify any asc/desc/nulls - so we need to inject it here
			// also means clause.Children()[0] cannot be used in the lookupExpr below
			asc, nullsFirst = true, false
			needle = clause
		}
		if needle == nil {
			needle = clause.Children()[0]
		}
		pos := lookupExpr(needle, q.Select)
		if pos == -1 {
			return fmt.Errorf("cannot sort by a column not in projections: %s", needle)
		}
		res.sortColumnsIdxs[j] = pos

		res.asc[j] = asc
		res.nullsfirst[j] = nullsFirst
	}

	sort.Sort(res)

	return nil
}

func RunSQL(db *database.Database, query string) (*Result, error) {
	q, err := expr.ParseQuerySQL(query)
	if err != nil {
		return nil, err
	}
	return Run(db, q)
}

// Run runs a given query against this database
// TODO: we have to differentiate between input errors and runtime errors (errors.Is?)
// the former should result in a 4xx, the latter in a 5xx
func Run(db *database.Database, q expr.Query) (*Result, error) {
	if len(q.Select) == 0 {
		return nil, errNoProjection
	}
	res := &Result{
		Schema: make([]column.Schema, 0, len(q.Select)),
		Data:   make([]column.Chunk, 0),
		Length: -1,
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

	// TODO(next): remove this, we've already incorporated a part of this into `aggregate`
	// only check that if q.Limit is not nil that it's non-negative, otherwise don't use it and leave it in `q`
	limit := -1
	if q.Limit != nil {
		if *q.Limit < 0 {
			return nil, fmt.Errorf("%w: %v", errInvalidLimitValue, *q.Limit)
		}
		limit = *q.Limit
	}
	if q.Aggregate != nil || allAggregations {
		if err := aggregate(db, ds, res, q); err != nil {
			return nil, err
		}

		return res, nil
	}
	// TODO(HIGHPRIO): this can really explode memory usage by running just `select * from bar order by foo`
	// one random idea: run reorder in the hot for loop whenever res.Length >> q.Limit, once done, call
	// res.Prune (new method) which takes first `q.Limit` rows in the result set and prunes them (first by using
	// rowIdxs and building a bitmap)
	// MIGHT be more performant to do this before the .Append... not sure now
	// TODO(next)/OPTIM: this is a good place to think about NOT materialising these rows
	// in case we need to sort them afterwards. Imagine `select * from foo order by bar desc limit 10`
	// we can either return everything, sort it all based on `bar` and then take the top 10... or we can
	// do something like top-k first (even if we have `where` clauses), discard most of our data and then
	// proceed as usual
	for _, stripe := range ds.Stripes {
		colnames := expr.ColumnsUsed(ds.Schema, q.Select...)
		if q.Filter != nil {
			colnames = append(colnames, expr.ColumnsUsed(ds.Schema, q.Filter)...)
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
			// only prune the filter if we're not reordering in the end
			if q.Order == nil && limit >= 0 && filter.Count() > limit {
				filter.KeepFirstN(limit)
			}
		} else {
			// TODO/ARCH: all this limit handling is a bit clunky, simplify it quite a bit
			if q.Order == nil && limit >= 0 && stripe.Length > limit {
				filter = bitmap.NewBitmap(stripe.Length)
				filter.Invert()
				filter.KeepFirstN(limit)
			}
		}
		if filter != nil && filter.Count() < loadFromStripe {
			loadFromStripe = filter.Count()
		}
		if q.Order == nil && limit >= 0 && limit < loadFromStripe {
			loadFromStripe = limit
		}
		if loadFromStripe == 0 {
			continue
		}
		if q.Order == nil {
			limit -= loadFromStripe
		}
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
	res.Length = res.Data[0].Len()
	if q.Order != nil {
		if err := reorder(res, q); err != nil {
			return nil, err
		}
		if q.Limit != nil && *q.Limit < res.Length {
			res.Length = *q.Limit
		}
	}

	return res, nil
}
