package database

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/kokes/smda/src/column"
)

var errPathNotEmpty = errors.New("path not empty, but does not contain a smda config file")
var errDatasetNotFound = errors.New("dataset not found")
var errColumnNotFound = errors.New("column not found in schema")

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

// NewDatabase initiates a new database object and binds it to a given directory. If the directory
// doesn't exist, it creates it. If it exists, it loads the data contained within.
func NewDatabase(config *Config) (*Database, error) {
	if config == nil {
		config = &Config{}
	}
	if config.WorkingDirectory == "" {
		// if no directory supplied, create a database in a temp directory
		tdir, err := os.MkdirTemp("", "smda_tmp")
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
	cfgFile := filepath.Join(abspath, "smda_db.json")
	if stat, err := os.Stat(abspath); err == nil && stat.IsDir() {
		// TODO(next): check smda_db.json and see if we can leverage it (overwrite Config, or at leaste merge them)
		// let's assume it's not a directory... fair?
		if _, err := os.Stat(cfgFile); err != nil {
			return nil, fmt.Errorf("%w: cannot initialise a database in %v (%v)", errPathNotEmpty, abspath, err)
		}
	} else {
		if err := os.MkdirAll(config.WorkingDirectory, os.ModePerm); err != nil {
			return nil, err
		}
		f, err := os.Create(cfgFile)
		if err != nil {
			return nil, err
		}
		if err := f.Close(); err != nil {
			return nil, err
		}
	}

	// many objects within the database get random IDs assigned, so we better seed at some point
	// ARCH: might get in the way in testing, we'll deal with it if it happens to be a problem
	rand.Seed(time.Now().UTC().UnixNano())
	db := &Database{
		Config:   config,
		Datasets: make([]*Dataset, 0),
	}

	if err := os.MkdirAll(db.manifestPath(nil), os.ModePerm); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(db.dataPath(), os.ModePerm); err != nil {
		return nil, err
	}

	// read manifests and load existing files
	manifests, err := os.ReadDir(db.manifestPath(nil))
	if err != nil {
		return nil, err
	}
	for _, manifest := range manifests {
		var ds Dataset
		f, err := os.Open(filepath.Join(db.manifestPath(nil), manifest.Name()))
		if err != nil {
			return nil, err
		}
		if err := json.NewDecoder(f).Decode(&ds); err != nil {
			return nil, err
		}
		if err := db.AddDataset(&ds); err != nil {
			return nil, err
		}
	}

	return db, nil
}

func (db *Database) manifestPath(ds *Dataset) string {
	root := filepath.Join(db.Config.WorkingDirectory, "manifests")
	if ds == nil {
		return root
	}
	return filepath.Join(root, ds.ID.String()+".json")
}
func (db *Database) dataPath() string {
	return filepath.Join(db.Config.WorkingDirectory, "data")
}

// Drop deletes all local data for a given Database
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

// Stripe only contains metadata about a given stripe, it has to be loaded
// separately to obtain actual data
type Stripe struct {
	Id      UID
	Length  int
	Offsets []uint32
}

// Dataset contains metadata for a given dataset, which at this point means a table
type Dataset struct {
	ID      UID         `json:"id"`
	Name    string      `json:"name"`
	Created int64       `json:"created_timestamp"`
	Schema  TableSchema `json:"schema"`
	// TODO/OPTIM: we need the following for manifests, but it's unnecessary for writing in our
	// web requests - remove it from there
	Stripes []Stripe `json:"stripes"`
}

// TableSchema is a collection of column schemas
type TableSchema []column.Schema

// LocateColumn returns a column within a schema - its position and definition; error is
// triggered if this column is not found or the schema is nil
func (schema *TableSchema) LocateColumn(s string) (int, column.Schema, error) {
	if schema != nil {
		for j, col := range []column.Schema(*schema) {
			if col.Name == s {
				return j, col, nil
			}
		}
	}
	return 0, column.Schema{}, fmt.Errorf("%w: %v", errColumnNotFound, s)
}

// LocateColumnCaseInsensitive works just like LocateColumn, but it ignores casing
// ARCH: we could have used strings.EqualFold, but a) we have one static input (s), so we can
//       amortise the case lowering, b) the extra correctness in EqualFold is irrelevant here,
//		 because of our column naming restrictions
func (schema *TableSchema) LocateColumnCaseInsensitive(s string) (int, column.Schema, error) {
	s = strings.ToLower(s)
	if schema != nil {
		for j, col := range []column.Schema(*schema) {
			// ARCH: this might be wrong - if we have a column "SomeColumn", we don't want "somecolumn" to match it, do we?
			if strings.ToLower(col.Name) == s {
				return j, col, nil
			}
		}
	}
	return 0, column.Schema{}, fmt.Errorf("%w: %v", errColumnNotFound, s)
}

// NewDataset creates a new empty dataset
func NewDataset() *Dataset {
	return &Dataset{ID: newUID(OtypeDataset), Created: time.Now().Unix()}
}

// DatasetPath returns the path of a given dataset (all the stripes are there)
// ARCH: consider merging this with dataPath based on a nullable dataset argument (like manifestPath)
func (db *Database) DatasetPath(ds *Dataset) string {
	return filepath.Join(db.dataPath(), ds.ID.String())
}

func (db *Database) stripePath(ds *Dataset, stripe Stripe) string {
	return filepath.Join(db.DatasetPath(ds), stripe.Id.String())
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
func (db *Database) AddDataset(ds *Dataset) error {
	db.Lock()
	db.Datasets = append(db.Datasets, ds)
	db.Unlock()

	f, err := os.Create(db.manifestPath(ds))
	if err != nil {
		return err
	}
	// ARCH/OPTIM: bufio? Though the manifests are likely to be small (or will they?)
	if err := json.NewEncoder(f).Encode(ds); err != nil {
		return err
	}

	return nil
}

// tests cover only "real" datasets, not the raw ones
func (db *Database) removeDataset(ds *Dataset) error {
	db.Lock()
	for j, dataset := range db.Datasets {
		if dataset == ds {
			db.Datasets = append(db.Datasets[:j], db.Datasets[j+1:]...)
			// if the dataset isn't found, this is a noop
			break
		}
	}
	// not deferring this - we're not throwing errors and we want to unlock
	// it before the end of the function (removing data might take a while)
	db.Unlock()

	for _, stripe := range ds.Stripes {
		if err := os.Remove(db.stripePath(ds, stripe)); err != nil {
			return err
		}
	}

	if err := os.Remove(db.manifestPath(ds)); err != nil {
		return err
	}

	// This might throw a "directory not empty" in the future as other datasets might claim
	// parts of this directory (if we start sharing stripes). But let's cross that bridge
	// when we get to it
	if err := os.Remove(db.DatasetPath(ds)); err != nil {
		return err
	}

	return nil
}
