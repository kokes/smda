package query

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/kokes/smda/src/bitmap"
	"github.com/kokes/smda/src/column"
	"github.com/kokes/smda/src/database"
	"github.com/kokes/smda/src/query/expr"
)

var errNoProjection = errors.New("no expressions specified to be selected")
var errInvalidLimitValue = errors.New("invalid limit value")
var errInvalidProjectionInAggregation = errors.New("selections in aggregating expressions need to be either the group by clauses or aggregating expressions (e.g. sum(foo))")
var errInvalidOrderClause = errors.New("invalid ORDER BY clause")
var errInvalidGroupbyClause = errors.New("invalid GROUP BY clause")
var errQueryNoDatasetIdentifiers = errors.New("query without a dataset has identifiers in the SELECT clause")

// Timer measures the duration of a given action/event
type Timer struct {
	Event string `json:"event"`
	// TODO: parentNode or something - this will require some identifiers/indexes
	//       meaning that "total_elapsed" will be the root node, the rest will be its children
	//       and we can support arbitrary nesting here, but let's start simple
	//       One alternative is to have a Timer field here (subtimers)
	Context string `json:"context"`
	// elapsed since the execution started (relative time)
	StartUs int64 `json:"start_us"`
	EndUs   int64 `json:"end_us"`
}

// Result holds the result of a query, at this point it's fairly literal - in the future we may want
// a Result to be a Dataset of its own (for better interoperability, persistence, caching etc.)
// ARCH/TODO: this is really a schema and `stripeData`, isn't it? Can we leverage that?
type Result struct {
	Schema column.TableSchema
	Length int
	Data   []column.Chunk
	Timers []Timer
	// ARCH: consider something like `stats` that will encapsulate this?
	executionStart time.Time
	bytesRead      int

	// this is used for sorting
	rowIdxs    []int
	asc        []bool
	nullsfirst []bool
	// this does not allow for sorting by things not materialised by projections (ARCH?)
	sortColumnsIdxs []int
}

// TODO: we may need to lock this if we introduce concurrency in Run()
func (res *Result) TimerStart(event, context string) func() {
	startUs := time.Now().Sub(res.executionStart).Microseconds()
	return func() {
		res.Timers = append(res.Timers, Timer{
			Event:   event,
			Context: context,
			StartUs: startUs,
			EndUs:   time.Now().Sub(res.executionStart).Microseconds(),
		})
	}
}

// Length might be much smaller than the data within (thanks to ORDER BY), so we should prune our columns
func (res *Result) Prune() {
	// take actual data length, not res.Length, which may be artificially low (that's the purpose here, to set
	// it low and discard all the other rows)
	bm := bitmap.NewBitmap(res.Data[0].Len())
	for j, el := range res.rowIdxs {
		if el < res.Length {
			bm.Set(j, true)
		}
	}
	for j, col := range res.Data {
		res.Data[j] = col.Prune(bm)
	}
	// TODO(next)/ARCH: the rowIdxs is all broken now... should we somehow clean it up?
	// `reorder` recreates it, so it's fine, but e.g. rowIdxs is used in serialisation, so
	// if we run Prune and then export... it might panic
}

// TODO(next): test this (at least that it's valid JSON, since we're doing all sorts of tricks here that could go wrong)
func (r *Result) MarshalJSON() ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	if _, err := buf.WriteString("{\n\t\"schema\": "); err != nil {
		return nil, err
	}
	if err := enc.Encode(r.Schema); err != nil {
		return nil, err
	}
	if _, err := buf.WriteString(",\"timers\": "); err != nil {
		return nil, err
	}
	if err := enc.Encode(r.Timers); err != nil {
		return nil, err
	}
	if _, err := buf.WriteString(fmt.Sprintf(",\n\"nrows\": %d", r.Length)); err != nil {
		return nil, err
	}
	if _, err := buf.WriteString(fmt.Sprintf(",\n\"bytes_read\": %d", r.bytesRead)); err != nil {
		return nil, err
	}

	// ARCH: there is no notion of order here - `foo asc, bar desc` is the same as the other way around
	// we might want to encode this order here at some point, so that the FE can react to it
	// ARCH: we used to guard this with `if len(r.sortColumnsIdxs) > 0 {`, but I guess we want to include
	// it in the JSON every time, so that we don't have to check for existence... or do we?
	sorting := make([]*string, len(r.Schema))
	for j, idx := range r.sortColumnsIdxs {
		order := "asc"
		if !r.asc[j] {
			order = "desc"
		}
		sorting[idx] = &order
	}
	// OPTIM: omit this if there's no ordering info
	if _, err := buf.WriteString(",\n\"ordering\": "); err != nil {
		return nil, err
	}
	if err := enc.Encode(sorting); err != nil {
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

func filterStripe(db *database.Database, ds *database.Dataset, stripe database.Stripe, filterExpr expr.Expression, colData map[string]column.Chunk) (*bitmap.Bitmap, error) {
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
func lookupExpr(needle expr.Expression, haystack []expr.Expression) int {
	ni, nl := needle.String(), ""
	if lab, ok := needle.(*expr.Relabel); ok {
		ni = lab.Children()[0].String()
		nl = lab.Label
	}
	for j, ex := range haystack {
		hi, hl := ex.String(), ""
		if lab, ok := ex.(*expr.Relabel); ok {
			hi = lab.Children()[0].String()
			hl = lab.Label
		}
		if ni == hi || (hl != "" && ni == hl) || (nl != "" && nl == hi) || (nl != "" && hl != "" && nl == hl) {
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
	var aggexprs []*expr.Function
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
		if err := expr.InitAggregator(aggexpr, ds.Schema); err != nil {
			return err
		}
	}

	columnNames := expr.ColumnsUsedMultiple(ds.Schema, append(q.Aggregate, q.Select...)...)
	if q.Filter != nil {
		columnNames = append(columnNames, expr.ColumnsUsedMultiple(ds.Schema, q.Filter)...)
	}
	groups := make(map[uint64]uint64)
	// ARCH: `nrc` and `rcs` are not very descriptive
	nrc := make([]column.Chunk, len(q.Aggregate))
	for _, stripe := range ds.Stripes {
		stripeLength := stripe.Length
		var filter *bitmap.Bitmap
		rcs := make([]column.Chunk, len(q.Aggregate))
		columnData, bytesRead, err := db.ReadColumnsFromStripeByNames(ds, stripe, columnNames)
		res.bytesRead += bytesRead
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
			// TODO: this is untested, because we have large stripes in testing
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
	res.Length = ret[0].Len()

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

	// all are equal, so just return true to avoid further sorting,
	// which wouldn't make a difference
	return true
}

func reorder(res *Result, q expr.Query) error {
	if res.Length < 0 {
		return errors.New("invalid structure of intermediate results")
	}
	res.rowIdxs = make([]int, res.Length)
	for j := 0; j < res.Length; j++ {
		res.rowIdxs[j] = j
	}
	res.asc = make([]bool, len(q.Order))
	res.nullsfirst = make([]bool, len(q.Order))
	res.sortColumnsIdxs = make([]int, len(q.Order))
	for j := 0; j < len(q.Order); j++ {
		clause := q.Order[j]
		asc, nullsFirst := true, false
		needle := clause
		if oby, ok := clause.(*expr.Ordering); ok {
			asc = oby.Asc
			nullsFirst = oby.NullsFirst
			needle = oby.Children()[0]
		}
		// TODO/ARCH: I wanted to change q.Order in place... but we can't create a new Ordering,
		// because `.inner` is private and I didn't want to expose it. But it might be the right way to go
		// TODO: test this properly (we have parsing tests, not implementation testing)
		if idx, ok := needle.(*expr.Integer); ok {
			needle = q.Select[idx.Value()-1] // no need to validate any more, already did that
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
	res := &Result{
		Schema:         make([]column.Schema, 0, len(q.Select)),
		Data:           make([]column.Chunk, 0),
		Length:         -1,
		executionStart: time.Now(),
	}
	stopExecTimer := res.TimerStart("total", "")
	stopPrepTimer := res.TimerStart("prep_query", "total")

	defer func() {
		defer stopExecTimer()
	}()

	if len(q.Select) == 0 {
		return nil, errNoProjection
	}

	// this is a special case of e.g. `SELECT 1`, `SELECT now()` etc.
	if q.Dataset == nil {
		for _, proj := range q.Select {
			if expr.HasIdentifiers(proj) {
				return nil, errQueryNoDatasetIdentifiers
			}
			rt, err := proj.ReturnType(nil)
			if err != nil {
				return nil, err
			}
			res.Schema = append(res.Schema, rt)
			col, err := expr.Evaluate(proj, 1, nil, nil)
			if err != nil {
				return nil, err
			}
			res.Data = append(res.Data, col)
		}

		res.Length = 1
		return res, nil
	}

	ds, err := db.GetDataset(q.Dataset.Name, q.Dataset.Version, q.Dataset.Latest)
	if err != nil {
		return nil, err
	}

	// expand `*` clauses
	// ARCH: we're mutating `q.Select`... we don't tend to do that here (it messes up printing it back)
	// consider having some optimisation here that will spit out a new `Query` and leave the old one intact
	var projs []expr.Expression
	for _, el := range q.Select {
		if idn, ok := el.(*expr.Identifier); ok && idn.Name == "*" {
			for _, el := range ds.Schema {
				col := expr.NewIdentifier(el.Name)
				// TODO(next): compare this namespace against our sources to make sure
				// we have this column? (or leave that to the query processor down below?)
				col.Namespace = idn.Namespace
				projs = append(projs, col)
			}
		} else {
			projs = append(projs, el)
		}
	}
	q.Select = projs

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

	if q.Order != nil {
		for _, proj := range q.Order {
			// order by clauses are NOT `expr.Ordering` by default - if they are plain `ORDER BY foo`,
			// they will just be expr.Identifier{foo} - so we need to unwrap them in case they are wrapped
			// like `exprOrdering{asc: true, inner: expr.Identifier{foo}}`
			if wrapped, ok := proj.(*expr.Ordering); ok {
				proj = wrapped.Children()[0]
			}

			// ORDER BY 1, 2
			if idx, ok := proj.(*expr.Integer); ok {
				n := idx.Value()
				if n < 1 || n > int64(len(q.Select)) {
					return nil, errInvalidOrderClause
				}
				continue
			}

			posS := lookupExpr(proj, q.Select)
			posG := -1
			if q.Aggregate != nil {
				posG = lookupExpr(proj, q.Aggregate)
			}

			if posS == -1 && posG == -1 {
				return nil, fmt.Errorf("%w: %v", errInvalidOrderClause, proj)
			}
		}
	}

	limit := -1
	if q.Limit != nil {
		if *q.Limit < 0 {
			return nil, fmt.Errorf("%w: %v", errInvalidLimitValue, *q.Limit)
		}
		limit = *q.Limit
	}

	stopPrepTimer()

	if q.Aggregate != nil || allAggregations {
		// edit GROUP BY 1, 2 in place (replace them by their respective columns)
		for j, agg := range q.Aggregate {
			if idx, ok := agg.(*expr.Integer); ok {
				n := idx.Value()
				if n < 1 || n > int64(len(q.Select)) {
					return nil, errInvalidGroupbyClause
				}
				q.Aggregate[j] = q.Select[n-1]
			}
		}

		stopAggTimer := res.TimerStart("aggregation", "total")
		if err := aggregate(db, ds, res, q); err != nil {
			return nil, err
		}
		stopAggTimer()

		return res, nil
	}

	// ARCH: this is quite coarse, but we cannot quite distinguish individual actions in the loop
	// below, because we'd have to somehow piece together timers from individual stripes (or leave
	// it on a stripe-by-stripe basis)
	stopQueryExecTimer := res.TimerStart("projection", "total")

	// OPTIM: if there's an ORDERBY, we sort/prune a given (filtered) stripe before appending it... so that
	// we don't append tons of data in case we have a LIMIT 10
	// But we still end up appending tons of data... shouldn't we do top-k or something?
	// We could also do a merge sort instead of sorting a list of sorted blocks
	// OPTIM/TODO(next): would be useful to allow for some limited concurrency here:
	//  We don't really want to do a map(process, ds.Stripes), that would grep huge amounts
	//  of data for each "SELECT foo FROM bar LIMIT 10" query. But we could process `n` stripes
	//  at a time, merge results, check if we can exit and perhaps continue with the next n (or perhaps
	//  evaluate after each stripe finishes and cancel the remaining processes, to avoid straggler issues).
	//  We can then map `n` to `numCPU` or something, but we could easily start with 1 to replicate current
	//  behaviour.
	for idx, stripe := range ds.Stripes {
		context := fmt.Sprintf("stripe_%02d_%s", idx, stripe.Id)
		stopStripeTimer := res.TimerStart(context, "projection")

		colnames := expr.ColumnsUsedMultiple(ds.Schema, q.Select...)
		if q.Filter != nil {
			colnames = append(colnames, expr.ColumnsUsedMultiple(ds.Schema, q.Filter)...)
		}
		stopDataTimer := res.TimerStart("read_stripe_data", context)
		columns, bytesRead, err := db.ReadColumnsFromStripeByNames(ds, stripe, colnames)
		res.bytesRead += bytesRead
		if err != nil {
			return nil, err
		}
		stopDataTimer()
		var filter *bitmap.Bitmap
		loadFromStripe := stripe.Length
		if q.Filter != nil {
			stopFilterTimer := res.TimerStart("filter_stripe", context)
			filter, err = filterStripe(db, ds, stripe, q.Filter, columns)
			if err != nil {
				return nil, err
			}
			// only prune the filter if we're not reordering in the end
			if q.Order == nil && limit >= 0 && filter.Count() > limit {
				filter.KeepFirstN(limit)
			}
			stopFilterTimer()
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
			stopStripeTimer()
			continue
		}
		if q.Order == nil {
			limit -= loadFromStripe
		}
		// we construct an intermediate column storage and sort it before adding it to our result
		// this will help us remove most of the data we don't need in case we're sorting it
		// OPTIM: either top-k to avoid most of the sort (might be tricky when sorting by multiple cols)
		// OPTIM: merge sort in the end, not append + sort (again, tricky for multiple cols)
		intermediate := &Result{}
		stopEvalTimer := res.TimerStart("evaluate_columns", context)
		for _, colExpr := range q.Select {
			col, err := expr.Evaluate(colExpr, loadFromStripe, columns, filter)
			if err != nil {
				return nil, err
			}

			intermediate.Data = append(intermediate.Data, col)
		}
		intermediate.Length = intermediate.Data[0].Len()
		stopEvalTimer()

		stopOrderingTimer := res.TimerStart("order_rows", context)
		if q.Order != nil && limit > 0 && intermediate.Length > limit {
			intermediate.Length = limit
			if err := reorder(intermediate, q); err != nil {
				return nil, err
			}
			intermediate.Prune()
		}
		stopOrderingTimer()

		stopAppendTimer := res.TimerStart("append_intermediate_results", context)
		for j, col := range intermediate.Data {
			if err := res.Data[j].Append(col); err != nil {
				return nil, err
			}
		}
		stopAppendTimer()

		// TODO/ARCH: we'll have to encapsulate stripe processing in some function, so that we don't have to faff
		// with these stop functions
		stopStripeTimer()
		if limit <= 0 {
			break
		}
	}
	stopQueryExecTimer()

	stopOrderingTimer := res.TimerStart("post_evaluation_ordering", "total")
	res.Length = res.Data[0].Len()
	if q.Order != nil {
		if err := reorder(res, q); err != nil {
			return nil, err
		}
		if q.Limit != nil && *q.Limit < res.Length {
			res.Length = *q.Limit
		}
	}
	stopOrderingTimer()

	return res, nil
}
