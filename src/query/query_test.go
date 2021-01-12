package query

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/kokes/smda/src/column"
	"github.com/kokes/smda/src/database"
	"github.com/kokes/smda/src/query/expr"
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

	qr, err := Run(db, q)
	if err != nil {
		t.Fatal(err)
	}
	if !(reflect.DeepEqual(qr.Columns, []string{}) && len(qr.Data) == 0) {
		t.Error("did not expect to get anything back")
	}
}

func selectExpr(cols []string) []*expr.Expression {
	ret := make([]*expr.Expression, 0, len(cols))
	for _, col := range cols {
		colExpr, err := expr.ParseStringExpr(col)
		if err != nil {
			panic(err)
		}
		ret = append(ret, colExpr)
	}
	return ret
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
	cols := selectExpr([]string{"foo", "bar", "baz"})
	q := Query{Select: cols, Dataset: ds.ID, Limit: &limit}

	qr, err := Run(db, q)
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
	cols := selectExpr([]string{"foo", "bar", "baz"})
	q := Query{Select: cols, Dataset: ds.ID}

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
			t.Errorf("expected error for negative values to be %v, got %v instead", errInvalidLimitValue, err)
		}
	}

	// non-negative limits
	for limit := 0; limit < 100; limit++ {
		q.Limit = &limit

		qr, err := Run(db, q)
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

func stringsToExprs(raw []string) ([]*expr.Expression, error) {
	ret := make([]*expr.Expression, 0, len(raw))
	for _, el := range raw {
		parsed, err := expr.ParseStringExpr(el)
		if err != nil {
			return nil, err
		}
		ret = append(ret, parsed)
	}
	return ret, nil
}

// TODO: not testing nulls here
// TODO: this only tests `aggregate`, not the whole query function - so we don't get schema checks e.g.
func TestBasicAggregation(t *testing.T) {
	tests := []struct {
		input   string
		aggexpr []string
		projs   []string
		output  string
	}{
		{"foo\na\nb\nc", []string{"foo"}, []string{"foo"}, "foo\na\nb\nc"},
		{"foo\na\na\na", []string{"foo"}, []string{"foo"}, "foo\na"},
		{"foo,bar\na,b\nb,a", []string{"foo"}, []string{"foo"}, "foo\na\nb"},
		{"foo,bar\na,b\nb,a", []string{"bar"}, []string{"bar"}, "bar\nb\na"},
		{"foo,bar\na,b\nc,d", []string{"foo", "bar"}, []string{"foo", "bar"}, "foo,bar\na,b\nc,d"},
		{"foo,bar\na,b\nd,a", []string{"foo", "bar"}, []string{"foo", "bar"}, "foo,bar\na,b\nd,a"},
		{"foo,bar\na,b\na,b", []string{"foo", "bar"}, []string{"foo", "bar"}, "foo,bar\na,b"},
		{"foo,bar\n1,2\n2,3", []string{"foo"}, []string{"foo"}, "foo\n1\n2"},
		{"foo,bar\nt,f\nt,f", []string{"foo"}, []string{"foo"}, "foo\ntrue"},
		{"foo,bar\n1,t\n2,f", []string{"foo"}, []string{"foo"}, "foo,bar\n1,true\n2,false"},
		// order preserving hashing
		{"foo,bar\na,b\nb,a", []string{"foo", "bar"}, []string{"foo", "bar"}, "foo,bar\na,b\nb,a"},
		{"foo,bar\n1,3\n3,1", []string{"foo", "bar"}, []string{"foo", "bar"}, "foo,bar\n1,3\n3,1"},
		{"foo,bar\n1.2,3\n3,1.2", []string{"foo", "bar"}, []string{"foo", "bar"}, "foo,bar\n1.2,3\n3,1.2"},
		{"foo,bar\nt,f\nf,t", []string{"foo", "bar"}, []string{"foo", "bar"}, "foo,bar\nt,f\nf,t"},
		// order preserving, with nulls
		{"foo,bar\nt,\nt,", []string{"foo", "bar"}, []string{"foo", "bar"}, "foo,bar\nt,"},
		{"foo,bar\n1,2\n,3\n,3\n,2", []string{"foo", "bar"}, []string{"foo", "bar"}, "foo,bar\n1,2\n,3\n,2"},
		{"foo,bar\n1.2,2\n,3.1\n,3.1\n,2", []string{"foo", "bar"}, []string{"foo", "bar"}, "foo,bar\n1.2,2\n,3.1\n,2"},
		// {"foo,bar\nt,1\n,1\nt,1", []string{"foo"}, []string{"foo"}, "foo\nt\n"}, // we're hitting go's encoding/csv again
		// nulls in aggregation:
		{"foo,bar\n,1\n0,2", []string{"foo"}, []string{"foo"}, "foo,bar\n,1\n0,2"},
		{"foo,bar\n1,1\n,2", []string{"foo"}, []string{"foo"}, "foo,bar\n1,1\n,2"},
		{"foo,bar\n,1\n.3,2", []string{"foo"}, []string{"foo"}, "foo,bar\n,1\n.3,2"},
		{"foo,bar\n,1\nt,2", []string{"foo"}, []string{"foo"}, "foo,bar\n,1\nt,2"},
		// basic expression aggregation
		{"foo,bar\n,1\nt,2", []string{"bar=1"}, []string{"bar=1"}, "bar=1\nt\nf"},
		// same as above, but the projection has extra whitespace (and it needs to still work)
		{"foo,bar\n,1\nt,2", []string{"bar=1"}, []string{"bar = 1"}, "bar=1\nt\nf"},
		{"foo,bar\n,1\nt,2", []string{"bar > 0"}, []string{"bar > 0"}, "bar>0\nt"},
		// TODO: nullable strings tests

		{"foo,bar\n1,12\n13,2\n1,3\n", []string{"foo"}, []string{"foo", "min(bar)"}, "foo,min(bar)\n1,3\n13,2"},
		{"foo,bar\n1,12.3\n13,2\n1,3.3\n", []string{"foo"}, []string{"foo", "min(bar)"}, "foo,min(bar)\n1,3.3\n13,2"},
		{"foo,bar\n1,12.3\n13,2\n1,3.3\n", []string{"foo"}, []string{"foo", "max(bar)"}, "foo,min(bar)\n1,12.3\n13,2"},
		{"foo,bar\n1,foo\n13,bar\n13,baz\n", []string{"foo"}, []string{"foo", "min(bar)"}, "foo,min(bar)\n1,foo\n13,bar"},
		{"foo,bar\n1,foo\n13,bar\n13,baz\n", []string{"foo"}, []string{"foo", "max(bar)"}, "foo,max(bar)\n1,foo\n13,baz"},
		{"foo,bar\n1,12.3\n13,2\n1,3.5\n", []string{"foo"}, []string{"foo", "sum(bar)"}, "foo,sum(bar)\n1,15.8\n13,2"},
		{"foo,bar\n1,5\n13,2\n1,10\n", []string{"foo"}, []string{"foo", "avg(bar)"}, "foo,avg(bar)\n1,7.5\n13,2"},
		{"foo,bar\n1,5\n13,2\n1,10\n", []string{"foo"}, []string{"foo", "count()"}, "foo,count(bar)\n1,2\n13,1"},
		{"foo,bar\n1,\n13,2\n1,10\n", []string{"foo"}, []string{"foo", "count()"}, "foo,count(bar)\n1,2\n13,1"},
		{"foo,bar\n1,12\n13,2\n1,10\n", []string{"foo"}, []string{"foo", "count(bar)"}, "foo,count(bar)\n1,2\n13,1"},
		// count() doesn't return nulls in values
		{"foo,bar\n1,\n13,2\n1,10\n3,\n", []string{"foo"}, []string{"foo", "count(bar)"}, "foo,count(bar)\n1,1\n13,1\n3,0"},
		// null handling (keys and values)
		{"foo,bar\n,12\n13,2\n1,3\n1,2\n", []string{"foo"}, []string{"foo", "min(bar)"}, "foo,min(bar)\n,12\n13,2\n1,2"},
		{"foo,bar\n1,\n13,2\n1,\n", []string{"foo"}, []string{"foo", "min(bar)"}, "foo,min(bar)\n1,\n13,2"},
		{"foo,bar\n1,\n,\n1,10\n,4\n,\n", []string{"foo"}, []string{"foo", "count(bar)"}, "foo,count(bar)\n1,1\n,1\n"},
		{"foo,bar\n1,\n,\n1,10\n,4\n,\n", []string{"foo"}, []string{"foo", "count()"}, "foo,count()\n1,2\n,3\n"},
		// we can't have sum(bool) yet, because bool aggregators can't have state in []int64
		// {"foo,bar\n1,t\n,\n1,f\n2,f\n2,t\n1,t\n", []string{"foo"}, []string{"foo", "sum(bar)"}, "foo,sumtbar()\n1,2\n2,2\n"},
		// dates
		{"foo,bar\n1,2020-01-30\n1,2020-02-20\n1,1979-12-31", []string{"foo"}, []string{"foo", "max(bar)"}, "foo,max(bar)\n1,2020-02-20\n"},
		{"foo,bar\n1,2020-01-30\n1,2020-02-20\n1,1979-12-31", []string{"foo"}, []string{"foo", "min(bar)"}, "foo,min(bar)\n1,1979-12-31\n"},
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

		aggexpr, err := stringsToExprs(test.aggexpr)
		if err != nil {
			t.Error(err)
			continue
		}
		projexpr, nil := stringsToExprs(test.projs)
		if err != nil {
			t.Error(err)
			continue
		}
		nrc, err := aggregate(db, ds, aggexpr, projexpr)
		if err != nil {
			t.Fatal(err)
		}
		if len(nrc) == 0 {
			t.Errorf("got no data from %v", test.input)
			continue
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
			if !column.ChunksEqual(col, expcol) {
				t.Errorf("[%d] failed to aggregate %v", testNo, test.input)
			}
		}
	}
}

func TestAggregationProjectionErrors(t *testing.T) {
	tests := []struct {
		input   string
		aggexpr []string
		projs   []string
	}{
		{"foo,bar,baz\n1,2,3\n", []string{"foo", "bar"}, []string{"foo*2", "bar"}},
		{"foo,bar,baz\n1,2,3\n", []string{"foo"}, []string{"bar"}},
		{"foo,bar,baz\n1,2,3\n", []string{"nullif(foo, 2)"}, []string{"foo"}},
		// {"foo,bar,baz\n1,2,3\n", []string{"2*foo"}, []string{"2*FOO"}}, // enable this once we get case insensitivity merged
	}

	for _, test := range tests {
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

		aggexpr, err := stringsToExprs(test.aggexpr)
		if err != nil {
			t.Error(err)
			continue
		}
		projexpr, err := stringsToExprs(test.projs)
		if err != nil {
			t.Error(err)
			continue
		}
		_, err = aggregate(db, ds, aggexpr, projexpr)
		if !errors.Is(err, errInvalidProjectionInAggregation) {
			t.Errorf("expecting projection %v and aggregation %v to result in errInvalidProjectionInAggregation, got %v instead", test.projs, test.aggexpr, err)
		}
	}
}

func TestBasicFiltering(t *testing.T) {
	tests := []struct {
		input            string
		columns          []string
		filterExpression string
		output           string
	}{
		// no testing against literals as we don't support literal chunks yet
		{"foo\na\nb\nc", []string{"foo"}, "foo = foo", "foo\na\nb\nc"},
		// {"foo\na\nb\nc", []string{"foo"}, "foo != foo", "foo"}, // no type inference for our `output`
		{"foo,bar\n1,4\n5,5\n10,4", []string{"foo"}, "foo > bar", "foo\n10"},
		{"foo,bar\n1,4\n5,5\n10,4", []string{"foo"}, "foo >= bar", "foo\n5\n10"},
	}

	for _, test := range tests {
		db, err := database.NewDatabase(nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := db.Drop(); err != nil {
				panic(err)
			}
		}()

		input, err := db.LoadDatasetFromReaderAuto(strings.NewReader(test.input))
		if err != nil {
			t.Fatal(err)
		}

		expected, err := db.LoadDatasetFromReaderAuto(strings.NewReader(test.output))
		if err != nil {
			t.Fatal(err)
		}

		var sel []*expr.Expression
		for _, col := range test.columns {
			parsed, err := expr.ParseStringExpr(col)
			if err != nil {
				t.Fatal(err)
			}
			sel = append(sel, parsed)
		}
		filter, err := expr.ParseStringExpr(test.filterExpression)
		if err != nil {
			t.Fatal(err)
		}

		q := Query{
			Select:  sel,
			Dataset: input.ID,
			Filter:  filter,
		}

		filtered, err := Run(db, q)
		if err != nil {
			t.Error(err)
			continue
		}
		expectedCols, err := db.ReadColumnsFromStripeByNames(expected, expected.Stripes[0], test.columns)
		if err != nil {
			t.Error(err)
			continue
		}

		if !reflect.DeepEqual(filtered.Data, expectedCols) {
			t.Errorf("expecting filter %v to result in %v, not %v", test.filterExpression, expectedCols, filtered.Data)
		}

	}
}
