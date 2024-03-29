package database

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestNewUidStringify(t *testing.T) {
	uid := newUID(OtypeDataset)
	suid := uid.String()

	if len(suid) != 18 {
		t.Fatalf("expecting stringified unique IDs to be 18 chars (9 bytes, but in hex), got:  %+v", suid)
	}
}

func TestNewUidJSONify(t *testing.T) {
	uid := newUID(OtypeDataset)
	dt, err := json.Marshal(uid)
	if err != nil {
		t.Fatal(err)
	}

	if len(dt) != 20 {
		t.Errorf("expecting JSONified unique IDs to be 20 chars (9 bytes, but in hex + quotes), got:  %+v", len(dt))
	}
	if !(dt[0] == '"' && dt[len(dt)-1] == '"') {
		t.Errorf("expecting JSONified unique IDs to be quoted, got %+v", string(dt))
	}
}

func TestNewUidDeJSONify(t *testing.T) {
	uid := newUID(OtypeDataset)
	dt, err := json.Marshal(uid)
	if err != nil {
		t.Fatal(err)
	}

	var uid2 UID
	if err := json.Unmarshal(dt, &uid2); err != nil {
		t.Fatal(err)
	}
	if uid2.Otype != uid.Otype {
		t.Errorf("expecting the type to be the same after a roundtrip, got: %+v", uid2.Otype)
	}
	if uid2.oid != uid.oid {
		t.Errorf("expecting the id to be the same after a roundtrip, got: %+v", uid2.oid)
	}
}

func TestInitDB(t *testing.T) {
	dirname := t.TempDir()
	for _, path := range []string{"foo", "bar", "baz"} {
		tdr := filepath.Join(dirname, path)
		if _, err := NewDatabase(tdr, nil); err != nil {
			t.Error(err)
		}
	}
}

func TestOpenExistingDB(t *testing.T) {
	// first let's initialise a new db
	tdr := filepath.Join(t.TempDir(), "new_db")
	if _, err := NewDatabase(tdr, nil); err != nil {
		t.Fatal(err)
	}
	// we should be able to open said db
	for j := 0; j < 3; j++ {
		if _, err := NewDatabase(tdr, nil); err != nil {
			t.Errorf("creating a database in an existing directory after it was initialised should not trigger an err, got %+v", err)
		}
	}
}

func TestInitTempDB(t *testing.T) {
	for j := 0; j < 10; j++ {
		db, err := NewDatabase("", nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := db.Drop(); err != nil {
				panic(err)
			}
		}()
	}
}

func TestDatasetNames(t *testing.T) {
	tests := []struct {
		before, expected string
	}{
		{"dataset", "dataset"},
		{"  dataset	", "dataset"},
		{"my dataset", "my_dataset"},
		{"my dataset...", "my_dataset"},
		{"...my dataset...", "my_dataset"},
		{"myLargeDataset", "my_large_dataset"},
		{"123", "dataset123"},
		{"1dataset", "dataset1dataset"},
		{"", "dataset"},
		{"foo.csv", "foo_csv"}, // ARCH: do we want this or just "foo"?
	}

	for _, test := range tests {
		ds := NewDataset(test.before)
		if ds.Name != test.expected {
			t.Errorf("expected dataset name '%v' to be cleaned up into '%v', got '%v' instead", test.before, test.expected, ds.Name)
		}
	}
}

func TestAddingDatasets(t *testing.T) {
	db, err := NewDatabase("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()
	ds := NewDataset("foobar")
	if err := db.AddDataset(ds); err != nil {
		t.Fatal(err)
	}

	ds2, err := db.GetDatasetLatest(ds.Name)
	if err != nil {
		t.Fatal(err)
	}
	if ds != ds2 {
		t.Fatal("roundtrip did not work out")
	}
}

func TestAddingDatasetsWithVersions(t *testing.T) {
	db, err := NewDatabase("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()
	n := 10
	dss := make([]*Dataset, 0, n)
	for j := 0; j < n; j++ {
		name := fmt.Sprintf("foobar%02d", j%3) // have datasets with various names
		ds := NewDataset(name)
		if err := db.AddDataset(ds); err != nil {
			t.Fatal(err)
		}
		dss = append(dss, ds)
	}
	last := dss[len(dss)-1]

	ds, err := db.GetDatasetLatest(last.Name)
	if err != nil {
		t.Fatal(err)
	}
	if ds.ID != last.ID {
		t.Fatalf("queried for `latest` (%v), got %v instead", last.ID, ds.ID)
	}

	for _, ds := range dss {
		rds, err := db.GetDatasetByVersion(ds.Name, ds.ID.String())
		if err != nil {
			t.Fatal(err)
		}
		if ds != rds {
			t.Fatal("did not get the dataset by name/version correctly")
		}
	}
}

func TestAddingDatasetsWithRestarts(t *testing.T) {
	db, err := NewDatabase("", nil)
	if err != nil {
		t.Fatal(err)
	}
	wdir := db.Config.WorkingDirectory
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()
	ds := NewDataset("foobar")
	if err := db.AddDataset(ds); err != nil {
		t.Fatal(err)
	}

	db2, err := NewDatabase(wdir, nil)
	if err != nil {
		t.Fatal(err)
	}

	ds2, err := db2.GetDatasetLatest(ds.Name)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db2.Drop(); err != nil {
			panic(err)
		}
	}()
	if !reflect.DeepEqual(ds, ds2) {
		t.Fatal("roundtrip did not work out")
	}
}

func TestRemovingDatasets(t *testing.T) {
	db, err := NewDatabase("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()
	data := strings.NewReader("foo,bar,baz\n1,2,3\n4,5,6")
	ds, err := db.LoadDatasetFromReaderAuto("foobar", data)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AddDataset(ds); err != nil {
		t.Fatal(err)
	}
	for _, stripe := range ds.Stripes {
		path := db.stripePath(ds, stripe)
		_, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
	}

	if err := db.removeDataset(ds); err != nil {
		t.Fatal(err)
	}

	_, err = db.GetDatasetLatest(ds.Name)
	if !errors.Is(err, errDatasetNotFound) {
		t.Error("should not be able to retrieve a deleted dataset")
	}

	// test that files were deleted - we don't need to check individual stripes, we can just
	// check the directory is no longer there
	if _, err := os.Stat(db.DatasetPath(ds)); !os.IsNotExist(err) {
		t.Errorf("expecting data to be deleted along with a dataset, but got: %+v", err)
	}

	if _, err := os.Stat(db.manifestPath(ds)); !os.IsNotExist(err) {
		t.Errorf("expecting manifests to be deleted along with a dataset, but got: %+v", err)
	}
}

func TestGettingNewDatasets(t *testing.T) {
	db, err := NewDatabase("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()
	data := strings.NewReader("foo,bar,baz\n1,2,3\n4,5,6")
	ds, err := db.LoadDatasetFromReaderAuto("foobar", data)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AddDataset(ds); err != nil {
		t.Fatal(err)
	}
	ds2, err := db.GetDatasetLatest(ds.Name)
	if err != nil {
		t.Fatal(err)
	}
	if ds2 != ds {
		t.Errorf("did not get the same dataset back")
	}
}
