package smda

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNewUidStringify(t *testing.T) {
	uid := newUid(otypeDataset)
	suid := uid.String()

	if len(suid) != 18 {
		t.Fatalf("expecting stringified unique IDs to be 18 chars (9 bytes, but in hex), got:  %v", suid)
	}
}

func TestNewUidJSONify(t *testing.T) {
	uid := newUid(otypeDataset)
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

func TestInitDB(t *testing.T) {
	dr, err := ioutil.TempDir("", "init_db_testing")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dr)
	for _, path := range []string{"foo", "bar", "baz"} {
		tdr := filepath.Join(dr, path)
		if _, err := NewDatabase(tdr); err != nil {
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
	if _, err := NewDatabase(tdr); err != nil {
		t.Fatal(err)
	}
	// we should not be able to init a new one in the same dir
	for j := 0; j < 3; j++ {
		if _, err := NewDatabase(tdr); err == nil {
			// TODO: should probably check for the right error here (though we're not wrapping them for now)
			t.Error("should not allow multiple dbs to be initialised in the same directory")
		}
	}
}

func TestInitTempDB(t *testing.T) {
	for j := 0; j < 10; j++ {
		db, err := NewDatabaseTemp()
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(db.WorkingDirectory)
	}
}

func TestAddingDatasets(t *testing.T) {
	db, err := NewDatabaseTemp()
	if err != nil {
		t.Fatal(err)
	}
	ds := &Dataset{ID: newUid(otypeDataset)}
	db.addDataset(ds)

	ds2, err := db.getDataset(ds.ID.String())
	if err != nil {
		t.Fatal(err)
	}
	if ds != ds2 {
		t.Fatal("roundtrip did not work out")
	}
}

func TestRemovingDatasets(t *testing.T) {
	db, err := NewDatabaseTemp()
	if err != nil {
		t.Fatal(err)
	}
	ds := &Dataset{ID: newUid(otypeDataset)}
	db.addDataset(ds)
	db.removeDataset(ds)

	_, err = db.getDataset(ds.ID.String())
	if err == nil {
		// TODO: should probably check for the right error (though we're not wrapping now)
		t.Fatal("should not be able to retrieve a deleted dataset")
	}
}

func TestGettingNewDatasets(t *testing.T) {
	db, err := NewDatabaseTemp()
	if err != nil {
		t.Fatal(err)
	}
	data := strings.NewReader("foo,bar,baz\n1,2,3\n4,5,6")
	ds, err := db.loadDatasetFromReaderAuto(data)
	if err != nil {
		t.Fatal(err)
	}
	ds2, err := db.getDataset(ds.ID.String())
	if err != nil {
		t.Fatal(err)
	}
	if ds2 != ds {
		t.Errorf("did not get the same dataset back")
	}
}
