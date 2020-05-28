package smda

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Database is the main struct that contains it all - notably the datasets' metadata and the webserver
// Having the webserver here makes it convenient for testing - we can spawn new servers at a moment's notice
type Database struct {
	sync.Mutex
	Datasets         []*Dataset
	server           *http.Server
	WorkingDirectory string
}

// NewDatabase initiates a new database in a directory, which cannot exist (we wouldn't know what to do with any of
// the existing files there)
func NewDatabase(workingDirectory string) (*Database, error) {
	abspath, err := filepath.Abs(workingDirectory)
	if err != nil {
		return nil, err
	}
	if stat, err := os.Stat(abspath); err == nil && stat.IsDir() {
		// this will serve as OpenDatabase in the future, once we learn how to resume operation
		return nil, fmt.Errorf("cannot initialise a database in %v, it already exists", abspath)
	}
	if err := os.MkdirAll(workingDirectory, os.ModePerm); err != nil {
		return nil, err
	}

	// many objects within the database get random IDs assigned, so we better seed at some point
	// might get in the way in testing, we'll deal with it if it happens to be a problem
	rand.Seed(time.Now().UTC().UnixNano())
	db := &Database{
		WorkingDirectory: workingDirectory,
		Datasets:         make([]*Dataset, 0),
	}
	db.setupRoutes()
	return db, nil
}

// NewDatabaseTemp creates a new database in your system's temporary directory, it's mostly used for testing
func NewDatabaseTemp() (*Database, error) {
	tdir, err := ioutil.TempDir("", "smda_tmp")
	if err != nil {
		return nil, err
	}
	return NewDatabase(filepath.Join(tdir, "smda_database"))
}

func (db *Database) Drop() error {
	return os.RemoveAll(db.WorkingDirectory)
}

// object types (used for UIDs)
type otype uint8

const (
	otypeNone otype = iota
	otypeDataset
	otypeStripe
	// when we start using IDs for columns and jobs and other objects, this will be handy
)

// UID is a unique ID for a given object, it's NOT a uuid
type UID struct {
	otype otype
	oid   uint64
}

func newUID(otype otype) UID {
	return UID{
		otype: otype,
		oid:   rand.Uint64(),
	}
}

func (uid UID) String() string {
	bf := make([]byte, 9)
	bf[0] = byte(uid.otype)
	binary.LittleEndian.PutUint64(bf[1:], uid.oid)
	return hex.EncodeToString(bf)
}

// MarshalJSON satisfies the Marshaler interface, so that we can automatically marshal
// UIDs as JSON
func (uid UID) MarshalJSON() ([]byte, error) {
	ret := make([]byte, 20) // 9 bytes (18 chars in hex) + 2 quotes
	copy(ret[1:], []byte(uid.String()))
	ret[0] = '"'
	ret[len(ret)-1] = '"'
	return ret, nil
}

// UnmarshalJSON satisfies the Unmarshaler interface
// (we need a pointer here, because we'll be writing to it)
func (uid *UID) UnmarshalJSON(data []byte) error {
	if len(data) != 20 {
		return errors.New("unexpected byte array used for UIDs")
	}
	data = data[1:19] // strip quotes
	unhexed := make([]byte, 9)
	dec, err := hex.Decode(unhexed, data)
	if err != nil {
		return err
	}
	if dec != len(unhexed) {
		return errors.New("failed to decode UID")
	}

	uid.otype = otype(unhexed[0])
	uid.oid = binary.LittleEndian.Uint64(unhexed[1:9])
	return nil
}

// Dataset contains metadata for a given dataset, which at this point means a table
type Dataset struct {
	ID            UID            `json:"id"`
	Name          string         `json:"name"`
	Schema        []columnSchema `json:"schema"`
	Stripes       []UID          `json:"-"`
	LocalFilepath string         `json:"-"`
}

func NewDataset() *Dataset {
	return &Dataset{ID: newUID(otypeDataset)}
}

// not efficient in this implementation, but we don't have a map-like structure
// to store our datasets - we keep them in a slice, so that we have predictable order
// -> we need a sorted map
func (db *Database) getDataset(datasetID UID) (*Dataset, error) {
	for _, dataset := range db.Datasets {
		if dataset.ID == datasetID {
			return dataset, nil
		}
	}
	return nil, fmt.Errorf("dataset not found: %v", datasetID)
}

// this is a pretty rare event, so we don't expect much contention
// it's just to avoid some issues when marshaling the object around in the API etc.
func (db *Database) addDataset(ds *Dataset) {
	db.Lock()
	db.Datasets = append(db.Datasets, ds)
	db.Unlock()
}

// TODO: test for deletion
func (db *Database) removeDataset(ds *Dataset) error {
	db.Lock()
	defer db.Unlock()
	for j, dataset := range db.Datasets {
		if dataset == ds {
			db.Datasets = append(db.Datasets[:j], db.Datasets[j+1:]...)
			break // TODO: what if dataset is not found? return an error that we'll ignore?
		}
	}
	// TODO: this path stitching is omnipresent, get rid of it
	// this directory contains all the stripes a given dataset has
	// but it might as well be a file (for raw datasets)
	localDir := filepath.Join(db.WorkingDirectory, ds.ID.String())

	for _, stripeID := range ds.Stripes {
		filename := filepath.Join(localDir, stripeID.String())
		if err := os.Remove(filename); err != nil {
			return err
		}
	}
	if err := os.Remove(localDir); err != nil {
		// TODO: ignore if "directory not empty"? Because other datasets might claim this directory
		// and we only want to remove it if it's actually empty, so this error is fine.
		return err
	}

	return nil
}
