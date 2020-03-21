package smda

import (
	"encoding/json"
	"errors"
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
}

// type FilterTree - to be used once we have AND and OR clauses
type FilterExpression struct {
	Column   string // this will be a projection, not just a column (e.g. NULLIF(a, b) > 3)
	Operator string // TODO: change to `operator` and add an unmarshaler
	Argument string // this might need to be an array perhaps, when we get BETWEEN etc.
}

func (fe *FilterExpression) UnmarshalJSON(data []byte) error {
	var raw []string
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if len(raw) != 3 {
		return fmt.Errorf("expecting three parts to a filter expression, got %v: %v", len(raw), raw)
	}
	fe.Column = raw[0]
	fe.Operator = raw[1] // convert to `operator` once we get that one working
	fe.Argument = raw[2]
	return nil
}

func (db *Database) Filter(ds *Dataset, fe *FilterExpression) ([]*Bitmap, error) {
	if fe.Operator != "=" {
		return nil, errors.New("TODO")
	}
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
		bm := col.Filter(opEqual, fe.Argument)
		bms = append(bms, bm)
	}
	return bms, nil
}

// QueryResult holds the result of a query, at this point it's fairly literal - in the future we may want
// a QueryResult to be a Dataset of its own (for better interoperability, persistence, caching etc.)
type QueryResult struct {
	Columns []string      `json:"columns"`
	Data    []typedColumn `json:"data"`
}

// TODO: we have to differentiate between input errors and runtime errors (errors.Is?)
// the former should result in a 4xx, the latter in a 5xx
func (db *Database) query(q Query) (*QueryResult, error) {
	res := &QueryResult{
		Columns: make([]string, 0),
		Data:    make([]typedColumn, 0),
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

	// so far just loading the first stripe, let's see where we get from here
	for stripeIndex, stripeID := range ds.Stripes[:1] {
		for j := range ds.Schema {
			col, err := db.readColumnFromStripe(ds, stripeID, j)
			if err != nil {
				return nil, err
			}
			if bms != nil {
				col = col.Prune(bms[stripeIndex])
			}
			res.Data = append(res.Data, col)
		}
	}

	return res, nil
}
