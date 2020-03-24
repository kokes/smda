package smda

import (
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestQueryingEmptyDatasets(t *testing.T) {
	db, err := NewDatabaseTemp()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(db.WorkingDirectory)
	ds := NewDataset()
	db.addDataset(ds)
	q := Query{Dataset: ds.ID, Limit: 100}

	qr, err := db.Query(q)
	if err != nil {
		t.Fatal(err)
	}
	if !(reflect.DeepEqual(qr.Columns, []string{}) && len(qr.Data) == 0) {
		t.Error("did not expect to get anything back")
	}
}

func TestBasicQueries(t *testing.T) {
	db, err := NewDatabaseTemp()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(db.WorkingDirectory)
	data := strings.NewReader("foo,bar,baz\n1,2,3\n4,5,6")
	ds, err := db.loadDatasetFromReaderAuto(data)
	if err != nil {
		t.Fatal(err)
	}
	db.addDataset(ds)
	q := Query{Dataset: ds.ID, Limit: 100}

	qr, err := db.Query(q)
	if err != nil {
		t.Fatal(err)
	}
	if !(reflect.DeepEqual(qr.Columns, []string{"foo", "bar", "baz"}) && len(qr.Data) == 3) {
		t.Error("expecting three columns of data")
	}
	firstCol := newColumnInts(false)
	firstCol.addValue("1")
	firstCol.addValue("4")
	if !reflect.DeepEqual(qr.Data[0], firstCol) {
		t.Errorf("first column does not match what's expected: %v vs. %v", qr.Data[0], firstCol)
	}
}

func TestLimitsInQueries(t *testing.T) {
	db, err := NewDatabaseTemp()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(db.WorkingDirectory)
	data := strings.NewReader("foo,bar,baz\n1,2,3\n4,5,6\n7,8,9")
	ds, err := db.loadDatasetFromReaderAuto(data)
	if err != nil {
		t.Fatal(err)
	}
	db.addDataset(ds)

	firstColRaw := []string{"1", "4", "7"}
	for limit := 0; limit < 100; limit++ {
		q := Query{Dataset: ds.ID, Limit: limit}

		qr, err := db.Query(q)
		if err != nil {
			t.Fatal(err)
		}
		if !(reflect.DeepEqual(qr.Columns, []string{"foo", "bar", "baz"}) && len(qr.Data) == 3) {
			t.Error("expecting three columns of data")
		}
		firstCol := newColumnInts(false)
		for j, val := range firstColRaw {
			if j >= limit {
				break
			}
			firstCol.addValue(val)
		}
		if !reflect.DeepEqual(qr.Data[0], firstCol) {
			t.Errorf("first column does not match what's expected: %v vs. %v", qr.Data[0], firstCol)
		}
	}
}
