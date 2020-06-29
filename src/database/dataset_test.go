package database

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNewUidStringify(t *testing.T) {
	uid := newUID(OtypeDataset)
	suid := uid.String()

	if len(suid) != 18 {
		t.Fatalf("expecting stringified unique IDs to be 18 chars (9 bytes, but in hex), got:  %v", suid)
	}
}

func TestNewUidJSONify(t *testing.T) {
	uid := newUID(OtypeDataset)
	dt, err := json.Marshal(uid)
	if err != nil {
		t.Fatal(err)
	}

	if len(dt) != 20 {
		t.Errorf("expecting JSONified unique IDs to be 20 chars (9 bytes, but in hex + quotes), got:  %v", len(dt))
	}
	if !(dt[0] == '"' && dt[len(dt)-1] == '"') {
		t.Errorf("expecting JSONified unique IDs to be quoted, got %v", string(dt))
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
		t.Errorf("expecting the type to be the same after a roundtrip, got: %v", uid2.Otype)
	}
	if uid2.oid != uid.oid {
		t.Errorf("expecting the id to be the same after a roundtrip, got: %v", uid2.oid)
	}
}

func TestInitDB(t *testing.T) {
	dr, err := ioutil.TempDir("", "init_db_testing")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dr)
	for _, path := range []string{"foo", "bar", "baz"} {
		tdr := filepath.Join(dr, path)
		if _, err := NewDatabase(&DatabaseConfig{WorkingDirectory: tdr}); err != nil {
			t.Error(err)
		}
	}
}

func TestInitExistingDB(t *testing.T) {
	dr, err := ioutil.TempDir("", "init_db_testing")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dr)
	// first let's initialise a new db
	tdr := filepath.Join(dr, "new_db")
	if _, err := NewDatabase(&DatabaseConfig{WorkingDirectory: tdr}); err != nil {
		t.Fatal(err)
	}
	// we should not be able to init a new one in the same dir
	for j := 0; j < 3; j++ {
		if _, err := NewDatabase(&DatabaseConfig{WorkingDirectory: tdr}); !errors.Is(err, errPathNotEmpty) {
			t.Errorf("creating a database in an existing directory should trigger errPathNotEmpty, got %v", err)
		}
	}
}

func TestInitTempDB(t *testing.T) {
	for j := 0; j < 10; j++ {
		db, err := NewDatabase(nil)
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
	db, err := NewDatabase(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()
	ds := NewDataset()
	db.AddDataset(ds)

	ds2, err := db.GetDataset(ds.ID)
	if err != nil {
		t.Fatal(err)
	}
	if ds != ds2 {
		t.Fatal("roundtrip did not work out")
	}
}

func TestRemovingDatasets(t *testing.T) {
	db, err := NewDatabase(nil)
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
	for _, stripeID := range ds.Stripes {
		path := db.stripePath(ds, stripeID)
		_, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
	}

	db.removeDataset(ds)

	_, err = db.GetDataset(ds.ID)
	if err == nil {
		// TODO: should probably check for the right error (though we're not wrapping now)
		t.Error("should not be able to retrieve a deleted dataset")
	}

	// test that files were deleted - we don't need to check individual stripes, we can just
	// check the directory is no longer there
	if _, err := os.Stat(db.DatasetPath(ds)); !os.IsNotExist(err) {
		t.Errorf("expecting data to be deleted along with a dataset, but got: %v", err)
	}
}

func TestGettingNewDatasets(t *testing.T) {
	db, err := NewDatabase(nil)
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
	ds2, err := db.GetDataset(ds.ID)
	if err != nil {
		t.Fatal(err)
	}
	if ds2 != ds {
		t.Errorf("did not get the same dataset back")
	}
}

// test LocateColumn
