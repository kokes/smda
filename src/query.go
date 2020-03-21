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

// TODO: maybe this doesn't need to return a dataset - if we're doing projections, grouping,
// having etc., we don't have to materialise this at this point (there could be tons of extra
// columns here) - let's just return a slice of bitmaps? We can run Dataset.Prune(bitmaps) later
func (db *Database) Filter(ds *Dataset, fe *FilterExpression) (*Dataset, error) {
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

	newDs := NewDataset()
	newDs.Schema = ds.Schema
	for _, stripe := range ds.Stripes {
		newStripe := newDataStripe()
		// first let's filter the column in question
		col, err := db.readColumnFromStripe(ds, stripe, colIndex)
		if err != nil {
			return nil, err
		}
		bm := col.Filter(opEqual, fe.Argument)
		// OPTIM: if bm.Count() is the same length as the columns in this stripe, we don't need to do this
		// now prune all the columns and assign to a new stripe
		for j := range ds.Schema {
			col, err := db.readColumnFromStripe(ds, stripe, j)
			if err != nil {
				return nil, err
			}
			newStripe.columns = append(newStripe.columns, col.Prune(bm))
		}
		// can we perhaps keep it in memory? given some condition?
		// TODO: also - this will live on - how do we gc this? and if we do gc it, how do we recreate it,
		// if somebody asks for it?
		// TODO: this is quite a serious issue at this point, because we don't even register this dataset
		// in a database, so there's no record of this, no way to gc it, reuse it or anything
		// perhaps give it a TTL and register it? or durable: false and a creation timestamp?
		if err := newStripe.writeToFile(db.WorkingDirectory, newDs.ID.String()); err != nil {
			return nil, err
		}
		newDs.Stripes = append(newDs.Stripes, newStripe.id)
	}
	return newDs, nil
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

	if q.Filter != nil {
		ds, err = db.Filter(ds, q.Filter)
		if err != nil {
			return nil, err
		}
	}

	for _, col := range ds.Schema {
		res.Columns = append(res.Columns, col.Name)
	}

	// so far just loading the first stripe, let's see where we get from here
	for _, stripeID := range ds.Stripes[:1] {
		for j := range ds.Schema {
			col, err := db.readColumnFromStripe(ds, stripeID, j)
			if err != nil {
				return nil, err
			}
			res.Data = append(res.Data, col)
		}
	}

	return res, nil
}
