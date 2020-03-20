package smda

// Query describes what we want to retrieve from a given dataset
// There are basically four places you need to edit in order to extend this:
// 1) The engine itself needs to support this functionality (usually a method on Dataset or typedColumn)
// 2) The query method has to be able to translate query parameters to the engine
// 3) The query endpoint handler needs to be able to process the incoming body
//    to the Query struct (the Unmarshaler should mostly take care of this)
// 4) The HTML/JS frontend needs to incorporate this in some way
type Query struct {
	Dataset UID `json:"dataset"`
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
