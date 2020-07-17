package query

import (
	"reflect"
	"strings"
	"testing"

	"github.com/kokes/smda/src/column"
	"github.com/kokes/smda/src/database"
)

func TestQueryingEmptyDatasets(t *testing.T) {
	db, err := database.NewDatabase(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()

	ds := database.NewDataset()
	db.AddDataset(ds)
	limit := 100
	q := Query{Dataset: ds.ID, Limit: &limit}

	qr, err := QueryData(db, q)
	if err != nil {
		t.Fatal(err)
	}
	if !(reflect.DeepEqual(qr.Columns, []string{}) && len(qr.Data) == 0) {
		t.Error("did not expect to get anything back")
	}
}

func TestBasicQueries(t *testing.T) {
	db, err := database.NewDatabase(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()

	data := strings.NewReader("foo,bar,baz\n1,2,3\n4,5,6")
	ds, err := db.LoadDatasetFromReaderAuto(data)
	if err != nil {
		t.Fatal(err)
	}
	db.AddDataset(ds)
	limit := 100
	q := Query{Dataset: ds.ID, Limit: &limit}

	qr, err := QueryData(db, q)
	if err != nil {
		t.Fatal(err)
	}
	if !(reflect.DeepEqual(qr.Columns, []string{"foo", "bar", "baz"}) && len(qr.Data) == 3) {
		t.Error("expecting three columns of data")
	}
	firstCol := column.NewChunkFromSchema(column.Schema{Dtype: column.DtypeInt})
	firstCol.AddValue("1")
	firstCol.AddValue("4")
	if !reflect.DeepEqual(qr.Data[0], firstCol) {
		t.Errorf("first column does not match what's expected: %v vs. %v", qr.Data[0], firstCol)
	}
}

// TODO: test that a limit omitted is equivalent to loading all data (test with and without filters)
// also test negative limits
func TestLimitsInQueries(t *testing.T) {
	db, err := database.NewDatabase(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()

	data := strings.NewReader("foo,bar,baz\n1,2,3\n4,5,6\n7,8,9")
	ds, err := db.LoadDatasetFromReaderAuto(data)
	if err != nil {
		t.Fatal(err)
	}
	db.AddDataset(ds)

	firstColRaw := []string{"1", "4", "7"}
	for limit := 0; limit < 100; limit++ {
		q := Query{Dataset: ds.ID, Limit: &limit}

		qr, err := QueryData(db, q)
		if err != nil {
			t.Fatal(err)
		}
		if !(reflect.DeepEqual(qr.Columns, []string{"foo", "bar", "baz"}) && len(qr.Data) == 3) {
			t.Error("expecting three columns of data")
		}
		firstCol := column.NewChunkFromSchema(column.Schema{Dtype: column.DtypeInt})
		if limit > len(firstColRaw) {
			firstCol.AddValues(firstColRaw)
		} else {
			firstCol.AddValues(firstColRaw[:limit])
		}
		if !reflect.DeepEqual(qr.Data[0], firstCol) {
			t.Errorf("first column does not match what's expected: %v vs. %v", qr.Data[0], firstCol)
		}
	}
}

// TODO: not testing nulls here
func TestBasicAggregation(t *testing.T) {
	tests := []struct {
		input     string
		aggregate []string
		output    string
	}{
		{"foo\na\nb\nc", []string{"foo"}, "foo\na\nb\nc"},
		{"foo\na\na\na", []string{"foo"}, "foo\na"},
		{"foo,bar\na,b\nb,a", []string{"foo"}, "foo\na\nb"},
		{"foo,bar\na,b\nb,a", []string{"bar"}, "bar\nb\na"},
		{"foo,bar\na,b\nc,d", []string{"foo", "bar"}, "foo,bar\na,b\nc,d"},
		{"foo,bar\na,b\nd,a", []string{"foo", "bar"}, "foo,bar\na,b\nd,a"},
		{"foo,bar\na,b\na,b", []string{"foo", "bar"}, "foo,bar\na,b"},
		{"foo,bar\n1,2\n2,3", []string{"foo"}, "foo\n1\n2"},
		{"foo,bar\nt,f\nt,f", []string{"foo"}, "foo\ntrue"},
		{"foo,bar\n1,t\n2,f", []string{"foo"}, "foo,bar\n1,true\n2,false"},
		// {"foo,bar\na,b\nb,a", []string{"foo", "bar"}, "foo,bar\na,b\nb,a"}, // TODO: enable once we add order-preserving hashing
		// nulls in aggregation:
		{"foo,bar\n,1\n0,2", []string{"foo"}, "foo,bar\n,1\n0,2"},
		{"foo,bar\n1,1\n,2", []string{"foo"}, "foo,bar\n1,1\n,2"},
		// {"foo,bar\n,1\n.3,2", []string{"foo"}, "foo,bar\n,1\n.3,2"}, // TODO: can't test floats as deepEqual doesn't like NaNs
		{"foo,bar\n,1\nt,2", []string{"foo"}, "foo,bar\n,1\nt,2"},
		// TODO: nullable strings tests
	}

	for testNo, test := range tests {
		db, err := database.NewDatabase(nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := db.Drop(); err != nil {
				panic(err)
			}
		}()

		ds, err := db.LoadDatasetFromReaderAuto(strings.NewReader(test.input))
		if err != nil {
			t.Fatal(err)
		}

		dso, err := db.LoadDatasetFromReaderAuto(strings.NewReader(test.output))
		if err != nil {
			t.Fatal(err)
		}

		nrc, err := Aggregate(db, ds, test.aggregate)
		if err != nil {
			t.Fatal(err)
		}

		for j, col := range nrc {
			// TODO: we can't just read the first stripe, we need to either
			//        1) select the given column and see if it matches
			//        2) create a helper method which tests for equality of two datasets (== schema, == each column
			//           in each stripe, ignore stripeIDs)
			// also, to test this, we need to initialise the db with MaxRowsPerStripe to a very low number to force creation of multiple stripes
			expcol, err := db.ReadColumnFromStripe(dso, dso.Stripes[0], j)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(col, expcol) {
				// 	log.Println(string(col.(*columnStrings).data))
				t.Errorf("[%d] failed to aggregate %v", testNo, test.input)
			}
		}
	}
}
