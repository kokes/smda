package database

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

	"github.com/kokes/smda/src/column"
)

var errPathNotEmpty = errors.New("path not empty")
var errDatasetNotFound = errors.New("dataset not found")

// Database is the main struct that contains it all - notably the datasets' metadata and the webserver
// Having the webserver here makes it convenient for testing - we can spawn new servers at a moment's notice
type Database struct {
	sync.Mutex
	Datasets []*Dataset
	Server   *http.Server
	Config   *Config
}

// Config sets some high level properties for a new Database. It's useful for testing or for passing
// settings based on cli flags.
type Config struct {
	WorkingDirectory  string
	MaxRowsPerStripe  int
	MaxBytesPerStripe int
}

// NewDatabase initiates a new database in a directory, which cannot exist (we wouldn't know what to do with any of
// the existing files there)
func NewDatabase(config *Config) (*Database, error) {
	if config == nil {
		config = &Config{}
	}
	if config.WorkingDirectory == "" {
		// if no directory supplied, create a database in a temp directory
		tdir, err := ioutil.TempDir("", "smda_tmp")
		if err != nil {
			return nil, err
		}
		config.WorkingDirectory = filepath.Join(tdir, "smda_database")
	}

	if config.MaxRowsPerStripe == 0 {
		config.MaxRowsPerStripe = 100_000
	}
	if config.MaxBytesPerStripe == 0 {
		config.MaxBytesPerStripe = 10_000_000
	}
	abspath, err := filepath.Abs(config.WorkingDirectory)
	if err != nil {
		return nil, err
	}
	if stat, err := os.Stat(abspath); err == nil && stat.IsDir() {
		// this will serve as OpenDatabase in the future, once we learn how to resume operation
		return nil, fmt.Errorf("cannot initialise a database in %v: %w", abspath, errPathNotEmpty)
	}
	if err := os.MkdirAll(config.WorkingDirectory, os.ModePerm); err != nil {
		return nil, err
	}

	// many objects within the database get random IDs assigned, so we better seed at some point
	// might get in the way in testing, we'll deal with it if it happens to be a problem
	rand.Seed(time.Now().UTC().UnixNano())
	db := &Database{
		Config:   config,
		Datasets: make([]*Dataset, 0),
	}
	return db, nil
}

// Drop deletes all data for a given Database
func (db *Database) Drop() error {
	return os.RemoveAll(db.Config.WorkingDirectory)
}

// ObjectType denotes what type an object is (or its ID) - dataset, stripe etc.
type ObjectType uint8

// object types are reflected in the UID - the first two hex characters define this object type,
// so it's clear what sort of object you're dealing with based on its prefix
const (
	OtypeNone ObjectType = iota
	OtypeDataset
	OtypeStripe
	// when we start using IDs for columns and jobs and other objects, this will be handy
)

// UID is a unique ID for a given object, it's NOT a uuid
type UID struct {
	Otype ObjectType
	oid   uint64
}

func newUID(Otype ObjectType) UID {
	return UID{
		Otype: Otype,
		oid:   rand.Uint64(),
	}
}

func (uid UID) String() string {
	bf := make([]byte, 9)
	bf[0] = byte(uid.Otype)
	binary.LittleEndian.PutUint64(bf[1:], uid.oid)
	return hex.EncodeToString(bf)
}

// MarshalJSON satisfies the Marshaler interface, so that we can automatically marshal
// UIDs as JSON
func (uid UID) MarshalJSON() ([]byte, error) {
	ret := make([]byte, 20) // 9 bytes (18 chars in hex) + 2 quotes
	copy(ret[1:], uid.String())
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

	uid.Otype = ObjectType(unhexed[0])
	uid.oid = binary.LittleEndian.Uint64(unhexed[1:9])
	return nil
}

// Dataset contains metadata for a given dataset, which at this point means a table
type Dataset struct {
	ID      UID         `json:"id"`
	Name    string      `json:"name"`
	Schema  TableSchema `json:"schema"`
	Stripes []UID       `json:"-"`
}

// TableSchema is a collection of column schemas
type TableSchema []column.Schema

// LocateColumn returns a column within a schema - its position and definition; error is
// triggered if this column is not found or the schema is nil
func (schema *TableSchema) LocateColumn(s string) (int, column.Schema, error) {
	if schema == nil {
		return 0, column.Schema{}, errors.New("empty schema cannot contain requested column")
	}
	for j, col := range []column.Schema(*schema) {
		if col.Name == s {
			return j, col, nil
		}
	}
	return 0, column.Schema{}, fmt.Errorf("column %v not found in schema", s)
}

// NewDataset creates a new empty dataset
func NewDataset() *Dataset {
	return &Dataset{ID: newUID(OtypeDataset)}
}

// DatasetPath returns the path of a given dataset (all the stripes are there)
func (db *Database) DatasetPath(ds *Dataset) string {
	return filepath.Join(db.Config.WorkingDirectory, ds.ID.String())
}

func (db *Database) stripePath(ds *Dataset, stripeID UID) string {
	return filepath.Join(db.DatasetPath(ds), stripeID.String())
}

// GetDataset retrieves a dataset based on its UID
// OPTIM: not efficient in this implementation, but we don't have a map-like structure
// to store our datasets - we keep them in a slice, so that we have predictable order
// -> we need a sorted map
func (db *Database) GetDataset(datasetID UID) (*Dataset, error) {
	for _, dataset := range db.Datasets {
		if dataset.ID == datasetID {
			return dataset, nil
		}
	}
	return nil, fmt.Errorf("dataset %v not found: %w", datasetID, errDatasetNotFound)
}

// AddDataset adds a Dataset to a Database
// this is a pretty rare event, so we don't expect much contention
// it's just to avoid some issues when marshaling the object around in the API etc.
func (db *Database) AddDataset(ds *Dataset) {
	db.Lock()
	db.Datasets = append(db.Datasets, ds)
	db.Unlock()
}

// tests cover only "real" datasets, not the raw ones
func (db *Database) removeDataset(ds *Dataset) error {
	db.Lock()
	defer db.Unlock()
	for j, dataset := range db.Datasets {
		if dataset == ds {
			db.Datasets = append(db.Datasets[:j], db.Datasets[j+1:]...)
			break // TODO: what if dataset is not found? return an error that we'll ignore?
		}
	}

	for _, stripeID := range ds.Stripes {
		if err := os.Remove(db.stripePath(ds, stripeID)); err != nil {
			return err
		}
	}
	if err := os.Remove(db.DatasetPath(ds)); err != nil {
		// TODO: ignore if "directory not empty"? Because other datasets might claim this directory
		// and we only want to remove it if it's actually empty, so this error is fine.
		return err
	}

	return nil
}
