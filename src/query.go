package smda

import (
	"errors"
)

// Query describes what we want to retrieve from a given dataset
type Query struct {
	Dataset string
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
	if q.Dataset == "" {
		return nil, errors.New("no dataset defined")
	}

	res := &QueryResult{
		Columns: make([]string, 0),
		Data:    make([]typedColumn, 0),
	}

	ds, err := db.getDataset(q.Dataset)
	if err != nil {
		return nil, err
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
