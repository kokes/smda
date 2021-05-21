package query

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/kokes/smda/src/column"
	"github.com/kokes/smda/src/database"
	"github.com/kokes/smda/src/query/expr"
)

func TestTheMostBasicQuery(t *testing.T) {
	db, err := database.NewDatabase("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()

	data := strings.NewReader("foo,bar,BAZ\n1,2,3\n4,5,6")
	ds, err := db.LoadDatasetFromReaderAuto(data)
	if err != nil {
		t.Fatal(err)
	}
	ds.Name = "foodata"
	if err := db.AddDataset(ds); err != nil {
		t.Fatal(err)
	}

	query := fmt.Sprintf("select foo, bar, baz from %v limit 100", ds.Name)
	qr, err := RunSQL(db, query)
	if err != nil {
		t.Fatal(err)
	}
	expschema := column.TableSchema{
		column.Schema{Name: "foo", Dtype: column.DtypeInt, Nullable: false},
		column.Schema{Name: "bar", Dtype: column.DtypeInt, Nullable: false},
		column.Schema{Name: "baz", Dtype: column.DtypeInt, Nullable: false},
	}
	if !(reflect.DeepEqual(qr.Schema, expschema) && len(qr.Data) == 3) {
		t.Errorf("expected schema %+v, got %+v instead", expschema, qr.Schema)
	}
	firstCol := column.NewChunkFromSchema(column.Schema{Dtype: column.DtypeInt})
	firstCol.AddValue("1")
	firstCol.AddValue("4")
	if !reflect.DeepEqual(qr.Data[0], firstCol) {
		t.Errorf("first column does not match what's expected: %+v vs. %+v", qr.Data[0], firstCol)
	}
}

// ARCH: we repeat quite a heavy setup - maybe abstract it out somehow?
func TestQueryNothing(t *testing.T) {
	db, err := database.NewDatabase("", nil)
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
	ds.Name = "foodata"
	if err := db.AddDataset(ds); err != nil {
		t.Fatal(err)
	}
	q := expr.Query{Select: nil, Dataset: &database.DatasetIdentifier{Name: ds.Name, Latest: true}}

	if _, err := Run(db, q); err != errNoProjection {
		t.Errorf("expected that selecting nothing will yield %v, got %v instead", errNoProjection, err)
	}
}

func TestLimitsInQueries(t *testing.T) {
	db, err := database.NewDatabase("", nil)
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
	ds.Name = "foodata"
	if err := db.AddDataset(ds); err != nil {
		t.Fatal(err)
	}

	firstColRaw := []string{"1", "4", "7"}
	cols, err := expr.ParseStringExprs("foo, bar, baz")
	if err != nil {
		t.Fatal(err)
	}
	q := expr.Query{Select: cols, Dataset: &database.DatasetIdentifier{Name: ds.Name, Latest: true}}

	// limit omitted
	qr, err := Run(db, q)
	if err != nil {
		t.Error(err)
	}
	if ds.Stripes[0].Length != len(firstColRaw) {
		t.Errorf("omitting a limit should result in getting all the data, got only %d rows", qr.Data[0].Len())
	}

	// negative limits
	for _, limit := range []int{-100, -20, -1} {
		q.Limit = &limit
		_, err := Run(db, q)
		if !errors.Is(err, errInvalidLimitValue) {
			t.Errorf("expected error for negative values to be %+v, got %+v instead", errInvalidLimitValue, err)
		}
	}

	// non-negative limits
	for limit := 0; limit < 100; limit++ {
		q.Limit = &limit

		qr, err := Run(db, q)
		if err != nil {
			t.Fatal(err)
		}
		expschema := column.TableSchema{
			column.Schema{Name: "foo", Dtype: column.DtypeInt, Nullable: false},
			column.Schema{Name: "bar", Dtype: column.DtypeInt, Nullable: false},
			column.Schema{Name: "baz", Dtype: column.DtypeInt, Nullable: false},
		}
		if !(reflect.DeepEqual(qr.Schema, expschema) && len(qr.Data) == 3) {
			t.Errorf("expected schema %+v, got %+v instead", expschema, qr.Schema)
		}
		firstCol := column.NewChunkFromSchema(column.Schema{Dtype: column.DtypeInt})
		if limit > len(firstColRaw) {
			firstCol.AddValues(firstColRaw)
		} else {
			firstCol.AddValues(firstColRaw[:limit])
		}
		if !reflect.DeepEqual(qr.Data[0], firstCol) {
			t.Errorf("first column does not match what's expected: %+v vs. %+v", qr.Data[0], firstCol)
		}
	}
}

func TestBasicQueries(t *testing.T) {
	tests := []struct {
		input  string
		query  string
		output string
	}{
		// basic aggregations
		{"foo\na\nb\nc", "SELECT foo FROM dataset GROUP BY foo", "foo\na\nb\nc"},
		{"foo\na\na\na", "SELECT foo FROM dataset GROUP BY foo", "foo\na"},
		{"foo,bar\na,b\nb,a", "SELECT foo FROM dataset GROUP BY foo", "foo\na\nb"},
		{"foo,bar\na,b\nb,a", "SELECT bar FROM dataset GROUP BY bar", "bar\nb\na"},
		{"foo,bar\na,b\nc,d", "SELECT foo, bar FROM dataset GROUP BY foo, bar", "foo,bar\na,b\nc,d"},
		{"foo,bar\na,b\nd,a", "SELECT foo, bar FROM dataset GROUP BY foo, bar", "foo,bar\na,b\nd,a"},
		{"foo,bar\na,b\na,b", "SELECT foo, bar FROM dataset GROUP BY foo, bar", "foo,bar\na,b"},
		{"foo,bar\n1,2\n2,3", "SELECT foo FROM dataset GROUP BY foo", "foo\n1\n2"},
		{"foo,bar\nt,f\nt,f", "SELECT foo FROM dataset GROUP BY foo", "foo\ntrue"},
		{"foo,bar\n1,t\n2,f", "SELECT foo FROM dataset GROUP BY foo", "foo,bar\n1,true\n2,false"},
		// order preserving hashing
		{"foo,bar\na,b\nb,a", "SELECT foo, bar FROM dataset GROUP BY foo, bar", "foo,bar\na,b\nb,a"},
		{"foo,bar\n1,3\n3,1", "SELECT foo, bar FROM dataset GROUP BY foo, bar", "foo,bar\n1,3\n3,1"},
		{"foo,bar\n1.2,3\n3,1.2", "SELECT foo, bar FROM dataset GROUP BY foo, bar", "foo,bar\n1.2,3\n3,1.2"},
		{"foo,bar\nt,f\nf,t", "SELECT foo, bar FROM dataset GROUP BY foo, bar", "foo,bar\nt,f\nf,t"},
		// order preserving, with nulls
		{"foo,bar\nt,\nt,", "SELECT foo, bar FROM dataset GROUP BY foo, bar", "foo,bar\nt,"},
		{"foo,bar\n1,2\n,3\n,3\n,2", "SELECT foo, bar FROM dataset GROUP BY foo, bar", "foo,bar\n1,2\n,3\n,2"},
		{"foo,bar\n1.2,2\n,3.1\n,3.1\n,2", "SELECT foo, bar FROM dataset GROUP BY foo, bar", "foo,bar\n1.2,2\n,3.1\n,2"},
		// {"foo,bar\nt,1\n,1\nt,1", "SELECT foo FROM dataset GROUP BY foo", "foo\nt\n"}, // we're hitting go's encoding/csv again
		// nulls in aggregation:
		{"foo,bar\n,1\n0,2", "SELECT foo FROM dataset GROUP BY foo", "foo,bar\n,1\n0,2"},
		{"foo,bar\n1,1\n,2", "SELECT foo FROM dataset GROUP BY foo", "foo,bar\n1,1\n,2"},
		{"foo,bar\n,1\n.3,2", "SELECT foo FROM dataset GROUP BY foo", "foo,bar\n,1\n.3,2"},
		{"foo,bar\n,1\nt,2", "SELECT foo FROM dataset GROUP BY foo", "foo,bar\n,1\nt,2"},
		// basic expression aggregation
		{"foo,bar\n,1\nt,2", "SELECT bar=1 FROM dataset GROUP BY bar=1", "bar=1\nt\nf"},
		// same as above, but the projection has extra whitespace (and it needs to still work)
		{"foo,bar\n,1\nt,2", "SELECT bar = 1 FROM dataset GROUP BY bar=1", "bar=1\nt\nf"},
		{"foo,bar\n,1\nt,2", "SELECT bar > 0 FROM dataset GROUP BY bar > 0", "bar>0\nt"},
		// TODO: nullable strings tests

		{"foo,bar\n1,12\n13,2\n1,3\n", "SELECT foo, min(bar) FROM dataset GROUP BY foo", "foo,min(bar)\n1,3\n13,2"},
		{"foo,bar\n1,12.3\n13,2\n1,3.3\n", "SELECT foo, min(bar) FROM dataset GROUP BY foo", "foo,min(bar)\n1,3.3\n13,2"},
		{"foo,bar\n1,12.3\n13,2\n1,3.3\n", "SELECT foo, max(bar) FROM dataset GROUP BY foo", "foo,min(bar)\n1,12.3\n13,2"},
		{"foo,bar\n1,foo\n13,bar\n13,baz\n", "SELECT foo, min(bar) FROM dataset GROUP BY foo", "foo,min(bar)\n1,foo\n13,bar"},
		{"foo,bar\n1,foo\n13,bar\n13,baz\n", "SELECT foo, max(bar) FROM dataset GROUP BY foo", "foo,max(bar)\n1,foo\n13,baz"},
		{"foo,bar\n1,12.3\n13,2\n1,3.5\n", "SELECT foo, sum(bar) FROM dataset GROUP BY foo", "foo,sum(bar)\n1,15.8\n13,2"},
		{"foo,bar\n1,5\n13,2\n1,10\n", "SELECT foo, avg(bar) FROM dataset GROUP BY foo", "foo,avg(bar)\n1,7.5\n13,2"},
		{"foo,bar\n1,5\n13,2\n1,10\n", "SELECT foo, count() FROM dataset GROUP BY foo", "foo,count(bar)\n1,2\n13,1"},
		{"foo,bar\n1,\n13,2\n1,10\n", "SELECT foo, count() FROM dataset GROUP BY foo", "foo,count(bar)\n1,2\n13,1"},
		{"foo,bar\n1,12\n13,2\n1,10\n", "SELECT foo, count(bar) FROM dataset GROUP BY foo", "foo,count(bar)\n1,2\n13,1"},
		// count() doesn't return nulls in values
		{"foo,bar\n1,\n13,2\n1,10\n3,\n", "SELECT foo, count(bar) FROM dataset GROUP BY foo", "foo,count(bar)\n1,1\n13,1\n3,0"},
		// null handling (keys and values)
		{"foo,bar\n,12\n13,2\n1,3\n1,2\n", "SELECT foo, min(bar) FROM dataset GROUP BY foo", "foo,min(bar)\n,12\n13,2\n1,2"},
		{"foo,bar\n1,\n13,2\n1,\n", "SELECT foo, min(bar) FROM dataset GROUP BY foo", "foo,min(bar)\n1,\n13,2"},
		{"foo,bar\n1,\n,\n1,10\n,4\n,\n", "SELECT foo, count(bar) FROM dataset GROUP BY foo", "foo,count(bar)\n1,1\n,1\n"},
		{"foo,bar\n1,\n,\n1,10\n,4\n,\n", "SELECT foo, count() FROM dataset GROUP BY foo", "foo,count()\n1,2\n,3\n"},
		// we can't have sum(bool) yet, because bool aggregators can't have state in []int64
		// {"foo,bar\n1,t\n,\n1,f\n2,f\n2,t\n1,t\n", "SELECT foo, sum(bar) FROM dataset GROUP BY foo", "foo,sumtbar()\n1,2\n2,2\n"},
		// dates
		{"foo,bar\n1,2020-01-30\n1,2020-02-20\n1,1979-12-31", "SELECT foo, max(bar) FROM dataset GROUP BY foo", "foo,max(bar)\n1,2020-02-20\n"},
		{"foo,bar\n1,2020-01-30\n1,2020-02-20\n1,1979-12-31", "SELECT foo, min(bar) FROM dataset GROUP BY foo", "foo,min(bar)\n1,1979-12-31\n"},
		{"foo,bar\n1,2020-01-30 12:34:56\n1,2020-02-20 00:00:00\n1,1979-12-31 19:01:57", "SELECT foo, min(bar) FROM dataset GROUP BY foo", "foo,min(bar)\n1,1979-12-31 19:01:57\n"},
		{"foo,bar\n1,2020-01-30 12:34:56\n1,1979-12-31 19:01:57.001\n1,1979-12-31 19:01:57.002", "SELECT foo, min(bar) FROM dataset GROUP BY foo", "foo,min(bar)\n1,1979-12-31 19:01:57.001\n"},
		{"foo,bar\n1,2020-01-30 12:34:56\n1,1979-12-31 19:01:57.001\n1,1979-12-31 19:01:57.0001", "SELECT foo, min(bar) FROM dataset GROUP BY foo", "foo,min(bar)\n1,1979-12-31 19:01:57.0001\n"},
		// case insensitivity
		{"foo,bar\n1,\n,\n1,10\n,4\n,\n", "SELECT foo, COUNT() FROM dataset GROUP BY foo", "foo,count()\n1,2\n,3\n"},
		{"foo,bar\n1,\n13,2\n1,\n", "SELECT foo, MIN(bar) FROM dataset GROUP BY foo", "foo,min(bar)\n1,\n13,2"},
		// no aggregating columns
		{"foo\n1\n2\n3\n", "SELECT sum(foo), max(foo) FROM dataset", "sum(foo),max(foo)\n6,3\n"},
		{"foo\n1\n2\n3\n", "SELECT count() FROM dataset", "count()\n3\n"},
		{"foo\n1\n2\n3\n", "SELECT count() - 2 FROM dataset", "count()\n1\n"},
		{"foo\n1\n2\n3\n", "SELECT 2-count() FROM dataset", "count()\n-1\n"},
		{"foo\n1\n2\n3\n", "SELECT count()*2 FROM dataset", "count()\n6\n"},
		{"foo\n1\n2\n3\n", "SELECT 2*count() FROM dataset", "count()\n6\n"},

		// basic filtering
		// no testing against literals as we don't support literal chunks yet
		// {"foo\na\nb\nc", "SELECT foo FROM dataset WHERE foo != foo", "foo"}, // no type inference for our `output`
		{"foo\na\nb\nc", "SELECT foo FROM dataset WHERE foo = foo", "foo\na\nb\nc"},
		{"foo,bar\n1,4\n5,5\n10,4", "SELECT foo FROM dataset WHERE foo > bar", "foo\n10"},
		{"foo,bar\n1,4\n5,5\n10,4", "SELECT foo FROM dataset WHERE foo >= bar", "foo\n5\n10"},
		{"foo,bar\n1,4\n5,5\n10,4", "SELECT foo FROM dataset WHERE 4 > 1", "foo\n1\n5\n10"},
		{"foo,bar\n,4\n5,5\n,6", "SELECT bar FROM dataset WHERE foo = null", "bar\n4\n6"},

		// filtering with groupbys
		{"foo,bar\n1,2\n3,4\n3,6", "SELECT foo, min(bar), max(bar) FROM dataset WHERE foo > 1 GROUP BY foo", "foo,min(bar),max(bar)\n3,4,6\n"},
		// TODO(next): test ORDER BY (incl. GROUP BY queries)
		// {"foo,bar\n,4\n5,5\n,6", "SELECT bar FROM dataset WHERE bar != null ORDER BY bar desc", "bar\n6\n5\n4"},
		// {"foo,bar\n,4\n5,5\n,6", "SELECT bar FROM dataset ORDER BY bar desc", "bar\n6\n5\n4"},
	}

	for testNo, test := range tests {
		db, err := database.NewDatabase("", nil)
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
		ds.Name = "dataset"
		if err := db.AddDataset(ds); err != nil {
			t.Fatal(err)
		}

		dso, err := db.LoadDatasetFromReaderAuto(strings.NewReader(test.output))
		if err != nil {
			t.Fatal(err)
		}
		if err := db.AddDataset(dso); err != nil {
			t.Fatal(err)
		}

		res, err := RunSQL(db, test.query)
		if err != nil {
			t.Fatal(err)
		}
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Data) == 0 {
			t.Errorf("got no data from %+v", test.input)
			continue
		}

		sr, err := database.NewStripeReader(db, dso, dso.Stripes[0])
		if err != nil {
			t.Fatal(err)
		}
		defer sr.Close()

		// we can't do this just yet, because column names get sanitised by default
		// if !reflect.DeepEqual(res.Schema, dso.Schema) {
		// 	t.Errorf("query %v resulted in a different schema - %v - than expected - %v", test.query, res.Schema, dso.Schema)
		// 	continue
		// }

		for j, col := range res.Data {
			// TODO: we can't just read the first stripe, we need to either
			//        1) select the given column and see if it matches
			//        2) create a helper method which tests for equality of two datasets (== schema, == each column
			//           in each stripe, ignore stripeIDs)
			// also, to test this, we need to initialise the db with MaxRowsPerStripe to a very low number to force creation of multiple stripes
			// ARCH: we might be better off just writing both datasets to CSV and comparing that byte for byte?
			// it might get hairy wrt nulls, but it will be straightforward otherwise
			expcol, err := sr.ReadColumn(j)
			if err != nil {
				t.Fatal(err)
			}
			// TODO(next): this doesn't take into account res.rowIdxs - we might have to compare JSON results
			// or maybe we'll implement (perhaps just here) something that physically reorders given Result.data
			if !column.ChunksEqual(col, expcol) {
				t.Errorf("[%d] failed to aggregate %+v", testNo, test.input)
			}
		}
	}
}

func TestProjections(t *testing.T) {
	tests := []struct {
		query      string
		projection []string
	}{
		{"select foo from dataset", []string{"foo"}},
		{"select 1+2 as foo from dataset", []string{"foo"}},
	}

	for _, test := range tests {
		db, err := database.NewDatabase("", nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := db.Drop(); err != nil {
				panic(err)
			}
		}()
		ds, err := db.LoadDatasetFromMap(map[string][]string{
			"foo": {"1", "2", "3"},
			"bar": {"1", "3", "4"},
		})
		if err != nil {
			t.Fatal(err)
		}
		ds.Name = "dataset"
		if err := db.AddDataset(ds); err != nil {
			t.Fatal(err)
		}

		res, err := RunSQL(db, test.query)
		if err != nil {
			t.Fatal(err)
		}
		if err != nil {
			t.Fatal(err)
		}
		_ = res
	}
}

func TestQuerySetup(t *testing.T) {
	tests := []struct {
		query string
		err   error
	}{
		{"SELECT foo*2, bar FROM dataset GROUP BY foo, bar", errInvalidProjectionInAggregation},
		{"SELECT bar FROM dataset GROUP BY foo", errInvalidProjectionInAggregation},
		{"SELECT foo FROM dataset GROUP BY nullif(foo, 2)", errInvalidProjectionInAggregation},
		{"SELECT foo FROM dataset ORDER by FOO", nil},
		{"SELECT foo FROM dataset ORDER by bar", errInvalidOrderClause},
		{"SELECT foo FROM dataset ORDER by foo, bar", errInvalidOrderClause},
		{"SELECT foo FROM dataset LIMIT 0", nil}, // cannot test -2, because that fails with a parser error
		// we get a parser issue, because we can get multiple where clauses only in JSON unmarshaling of queries
		// {"SELECT foo FROM dataset WHERE foo > 0, foo < 3", errInvalidFilter},

		// relabeling can be tricky, especially when looking up columns across parts of the query - these are all legal
		// BUT this doesn't work the same for WHERE clauses - we cannot filter on relabeled fields
		{"SELECT foo AS bar FROM dataset GROUP BY foo", nil},
		{"SELECT foo AS bar FROM dataset GROUP BY bar", nil},
		{"SELECT foo AS bar FROM dataset ORDER BY foo", nil},
		{"SELECT foo AS bar FROM dataset ORDER BY bar", nil},
	}

	for _, test := range tests {
		db, err := database.NewDatabase("", nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := db.Drop(); err != nil {
				panic(err)
			}
		}()

		ds, err := db.LoadDatasetFromReaderAuto(strings.NewReader("foo,bar,baz\n1,2,3\n"))
		if err != nil {
			t.Fatal(err)
		}
		ds.Name = "dataset"
		if err := db.AddDataset(ds); err != nil {
			t.Fatal(err)
		}

		if _, err := RunSQL(db, test.query); !errors.Is(err, test.err) {
			t.Errorf("expecting query %v to result in %v, got %+v instead", test.query, test.err, err)
		}
	}
}
