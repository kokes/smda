package smda

import (
	"encoding/binary"
	"encoding/hex"
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

// object types (used for UIDs)
type otype uint8

const (
	otypeNone otype = iota
	otypeDataset
	otypeStripe
	// when we start using IDs for columns and jobs and other objects, this will be handy
)

// NOT uuid
type uid struct {
	otype otype
	oid   uint64
}

func newUID(otype otype) uid {
	return uid{
		otype: otype,
		oid:   rand.Uint64(),
	}
}

func (uid uid) String() string {
	bf := make([]byte, 9)
	bf[0] = byte(uid.otype)
	binary.LittleEndian.PutUint64(bf[1:], uid.oid)
	return hex.EncodeToString(bf)
}

func (uid uid) MarshalJSON() ([]byte, error) {
	ret := make([]byte, 20) // 9 bytes (18 chars in hex) + 2 quotes
	copy(ret[1:], []byte(uid.String()))
	ret[0] = '"'
	ret[len(ret)-1] = '"'
	return ret, nil
}

// Dataset contains metadata for a given dataset, which at this point means a table
type Dataset struct {
	ID            uid            `json:"id"`
	Name          string         `json:"name"`
	Schema        []columnSchema `json:"schema"`
	Stripes       []uid          `json:"-"`
	LocalFilepath string         `json:"-"`
}

// not efficient in this implementation, but we don't have a map-like structure
// to store our datasets - we keep them in a slice, so that we have predictable order
// -> we need a sorted map
func (db *Database) getDataset(datasetID string) (*Dataset, error) {
	for _, dataset := range db.Datasets {
		if dataset.ID.String() == datasetID {
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

func (db *Database) removeDataset(ds *Dataset) {
	db.Lock()
	defer db.Unlock()
	for j, dataset := range db.Datasets {
		if dataset == ds {
			db.Datasets = append(db.Datasets[:j], db.Datasets[j+1:]...)
			break
		}
	}
	// TODO: remove data at some point
}
