package smda

import (
	"fmt"
)

// Query describes what we want to retrieve from a given dataset
// There are basically four places you need to edit (and test!) in order to extend this:
// 1) The engine itself needs to support this functionality (usually a method on Dataset or typedColumn)
// 2) The query method has to be able to translate query parameters to the engine
// 3) The query endpoint handler needs to be able to process the incoming body
//    to the Query struct (the Unmarshaler should mostly take care of this)
// 4) The HTML/JS frontend needs to incorporate this in some way
type Query struct {
	Dataset UID               `json:"dataset"`
	Filter  *FilterExpression `json:"filter"`
	Limit   int               `json:"limit"`
}

// type FilterTree - to be used once we have AND and OR clauses
type FilterExpression struct {
	Column   string   `json:"column"`   // this will be a projection, not just a column (e.g. NULLIF(a, b) > 3)
	Operator operator `json:"operator"` // TODO: change to `operator` and add an unmarshaler
	Argument string   `json:"arg"`      // this might need to be an array perhaps, when we get BETWEEN etc.
}

func (db *Database) Filter(ds *Dataset, fe *FilterExpression) ([]*Bitmap, error) {
	colIndex := -1
	for j, col := range ds.Schema {
		if col.Name == fe.Column {
			colIndex = j
			break
		}
	}
	if colIndex == -1 {
		return nil, fmt.Errorf("could not filter out `%v` in dataset %v, column not found", fe.Column, ds.ID)
	}

	bms := make([]*Bitmap, 0, len(ds.Stripes))
	for _, stripe := range ds.Stripes {
		col, err := db.readColumnFromStripe(ds, stripe, colIndex)
		if err != nil {
			return nil, err
		}
		// TODO: the thing with typedColumn.Filter not returning an error is that it panic
		// when using a non-supported operator - this does not lead to good user experience, plus
		// it allows the user to crash the system without a great logging experience
		bm := col.Filter(fe.Operator, fe.Argument)
		bms = append(bms, bm)
	}
	return bms, nil
}

// QueryResult holds the result of a query, at this point it's fairly literal - in the future we may want
// a QueryResult to be a Dataset of its own (for better interoperability, persistence, caching etc.)
type QueryResult struct {
	Columns []string        `json:"columns"`
	Data    [][]typedColumn `json:"data"`
}

// TODO: we have to differentiate between input errors and runtime errors (errors.Is?)
// the former should result in a 4xx, the latter in a 5xx
func (db *Database) query(q Query) (*QueryResult, error) {
	res := &QueryResult{
		Columns: make([]string, 0),
		Data:    make([][]typedColumn, 0),
	}

	ds, err := db.getDataset(q.Dataset)
	if err != nil {
		return nil, err
	}

	var bms []*Bitmap
	if q.Filter != nil {
		bms, err = db.Filter(ds, q.Filter)
		if err != nil {
			return nil, err
		}
	}

	for _, col := range ds.Schema {
		res.Columns = append(res.Columns, col.Name)
	}

	limit := q.Limit
	for stripeIndex, stripeID := range ds.Stripes {
		// if no relevant data in this stripe, skip it
		if bms != nil {
			filteredLen := bms[stripeIndex].Count()
			if filteredLen == 0 {
				continue
			}
			if filteredLen > limit {
				bms[stripeIndex].KeepFirstN(limit)
				limit -= filteredLen
			}
		}
		stripeData := make([]typedColumn, 0, len(ds.Schema))
		var bmnf *Bitmap // bitmap for non-filtered data - I really dislike the way this is handled (TODO)
		for j := range ds.Schema {
			col, err := db.readColumnFromStripe(ds, stripeID, j)
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
			if j == 0 && bms == nil {
				ln := int(col.Len())
				if ln <= limit {
					limit -= ln
				} else {
					bmnf = NewBitmap(ln)
					bmnf.invert()
					bmnf.KeepFirstN(limit)
				}
			}
			if bmnf != nil {
				col = col.Prune(bmnf)
			}
			stripeData = append(stripeData, col)
		}
		if bmnf != nil {
			limit -= bmnf.Count()
			bmnf = nil
		}
		res.Data = append(res.Data, stripeData)
		if limit <= 0 {
			break
		}
	}

	return res, nil
}
