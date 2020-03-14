package smda

import (
	"errors"
)

type Query struct {
	Dataset string
}

type QueryResult struct {
	Columns []string      `json:"columns"`
	Data    []typedColumn `json:"data"`
}

// TODO: we have to differentiate between input errors and runtime errors (errors.Is?)
// the former should result in a 4xx, the latter in a 5xx
func (d *Database) query(q Query) (*QueryResult, error) {
	if q.Dataset == "" {
		return nil, errors.New("no dataset defined")
	}

	res := &QueryResult{
		Columns: make([]string, 0),
		Data:    make([]typedColumn, 0),
	}

	ds, err := d.getDataset(q.Dataset)
	if err != nil {
		return nil, err
	}

	for _, col := range ds.Schema {
		res.Columns = append(res.Columns, col.Name)
	}

	// so far just loading the first stripe, let's see where we get from here
	for _, stripeId := range ds.Stripes[:1] {
		for j := range ds.Schema {
			col, err := d.readColumnFromStripe(ds, stripeId, j)
			if err != nil {
				return nil, err
			}
			res.Data = append(res.Data, col)
		}
	}

	return res, nil
}
