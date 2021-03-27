package database

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/kokes/smda/src/column"
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
	// REFACTOR: use t.TempDir here and everywhere else (go 1.15+)
	dr, err := os.MkdirTemp("", "init_db_testing")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dr)
	for _, path := range []string{"foo", "bar", "baz"} {
		tdr := filepath.Join(dr, path)
		if _, err := NewDatabase(tdr, nil); err != nil {
			t.Error(err)
		}
	}
}

func TestOpenExistingDB(t *testing.T) {
	dr, err := os.MkdirTemp("", "init_db_testing")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dr)
	// first let's initialise a new db
	tdr := filepath.Join(dr, "new_db")
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
	ds := NewDataset()
	if err := db.AddDataset(ds); err != nil {
		t.Fatal(err)
	}

	ds2, err := db.GetDataset(ds.ID)
	if err != nil {
		t.Fatal(err)
	}
	if ds != ds2 {
		t.Fatal("roundtrip did not work out")
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
	ds := NewDataset()
	if err := db.AddDataset(ds); err != nil {
		t.Fatal(err)
	}

	db2, err := NewDatabase(wdir, nil)
	if err != nil {
		t.Fatal(err)
	}

	ds2, err := db2.GetDataset(ds.ID)
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
	ds, err := db.LoadDatasetFromReaderAuto(data)
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

	_, err = db.GetDataset(ds.ID)
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
	ds, err := db.LoadDatasetFromReaderAuto(data)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AddDataset(ds); err != nil {
		t.Fatal(err)
	}
	ds2, err := db.GetDataset(ds.ID)
	if err != nil {
		t.Fatal(err)
	}
	if ds2 != ds {
		t.Errorf("did not get the same dataset back")
	}
}

func TestLocateColumn(t *testing.T) {
	tests := []struct {
		caseSensitive bool
		cols          []string
		lookup        string
		expectedIdx   int
		err           error
	}{
		// case insensitive
		{false, []string{"foo", "bar", "baz"}, "bar", 1, nil},
		{false, []string{"foo", "bar", "baz"}, "foo", 0, nil},
		{false, []string{"foo", "bar", "baz"}, "boo", 0, errColumnNotFound}, // the idx doesn't matter here
		{false, []string{}, "bar", 0, errColumnNotFound},
		// case sensitive
		{true, []string{}, "bar", 0, errColumnNotFound},
		{true, []string{"foo", "bar", "baz"}, "bar", 1, nil},
		{true, []string{"foo", "bar", "baz"}, "baz", 2, nil},
		{true, []string{"foo", "bar", "baz"}, "BAz", 2, nil},
		{true, []string{"foo", "bar", "baz"}, "BAR", 1, nil},
		{true, []string{"foo", "BAR", "baz"}, "BAR", 1, nil},
		{true, []string{"foo", "BAr", "baz"}, "bAR", 1, nil},
	}

	for _, test := range tests {
		var schema TableSchema
		for _, col := range test.cols {
			schema = append(schema, column.Schema{Name: col})
		}
		var (
			idx int
			err error
		)
		if test.caseSensitive {
			idx, _, err = schema.LocateColumnCaseInsensitive(test.lookup)
		} else {
			idx, _, err = schema.LocateColumn(test.lookup)
		}
		if !errors.Is(err, test.err) {
			t.Errorf("expected looking up %+v in %+v to return %+v, got %+v instead", test.lookup, test.cols, test.err, err)
			continue
		}
		if idx != test.expectedIdx {
			t.Errorf("expected looking up %+v in %+v to return idx %+v, got %+v instead", test.lookup, test.cols, test.expectedIdx, idx)
		}
	}
}
