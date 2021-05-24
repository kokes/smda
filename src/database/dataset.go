package database

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/kokes/smda/src/column"
)

var errPathNotEmpty = errors.New("path not empty, but does not contain a smda config file")
var errDatasetNotFound = errors.New("dataset not found")

// Database is the main struct that contains it all - notably the datasets' metadata and the webserver
// Having the webserver here makes it convenient for testing - we can spawn new servers at a moment's notice
type Database struct {
	sync.Mutex
	Datasets    []*Dataset
	ServerHTTP  *http.Server
	ServerHTTPS *http.Server
	Config      *Config
}

// Config sets some high level properties for a new Database. It's useful for testing or for passing
// settings based on cli flags.
type Config struct {
	WorkingDirectory  string `json:"-"` // not exposing this in our json representation as the db can be moved around
	CreatedTimestamp  int64  `json:"created_timestamp"`
	DatabaseID        UID    `json:"database_id"`
	MaxRowsPerStripe  int    `json:"max_rows_per_stripe"`
	MaxBytesPerStripe int    `json:"max_bytes_per_stripe"`
}

// NewDatabase initiates a new database object and binds it to a given directory. If the directory
// doesn't exist, it creates it. If it exists, it loads the data contained within.
func NewDatabase(wdir string, overrides *Config) (*Database, error) {
	// many objects within the database get random IDs assigned, so we better seed at some point
	// ARCH: might get in the way in testing, we'll deal with it if it happens to be a problem
	rand.Seed(time.Now().UTC().UnixNano())

	config := &Config{WorkingDirectory: wdir, CreatedTimestamp: time.Now().UTC().Unix()}
	if wdir == "" {
		// if no directory supplied, create a database in a temp directory
		// ARCH: we'll probably do this in $HOME in the future
		tdir, err := os.MkdirTemp("", "smda_tmp")
		if err != nil {
			return nil, err
		}
		config.WorkingDirectory = filepath.Join(tdir, "smda_database")
	}

	abspath, err := filepath.Abs(config.WorkingDirectory)
	if err != nil {
		return nil, err
	}
	cfgPath := filepath.Join(abspath, "smda_db.json")
	// TODO: test how we recover these values from a given file, how we can override them etc.
	if stat, err := os.Stat(abspath); err == nil && stat.IsDir() {
		f, err := os.Open(cfgPath)
		if err != nil {
			return nil, fmt.Errorf("%w: cannot initialise a database in %v (%v)", errPathNotEmpty, abspath, err)
		}

		if err := json.NewDecoder(f).Decode(&config); err != nil {
			return nil, err
		}
	}
	// ARCH: allow overrides of database UIDs?
	if overrides != nil {
		if overrides.MaxRowsPerStripe != 0 {
			config.MaxRowsPerStripe = overrides.MaxRowsPerStripe
		}
		if overrides.MaxBytesPerStripe != 0 {
			config.MaxBytesPerStripe = overrides.MaxBytesPerStripe
		}
	}

	if config.MaxRowsPerStripe == 0 {
		config.MaxRowsPerStripe = 100_000
	}
	if config.MaxBytesPerStripe == 0 {
		config.MaxBytesPerStripe = 10_000_000
	}
	if config.DatabaseID.Otype == OtypeNone {
		config.DatabaseID = newUID(OtypeDatabase)
	}

	if err := os.MkdirAll(config.WorkingDirectory, os.ModePerm); err != nil {
		return nil, err
	}
	// write this new configuration to a json file (that may have existed already)
	// ARCH: test if the contents are the same as what we've created and don't write in that case (just to save some mtime confusion)
	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	enc.SetIndent("", "  ")
	if err := enc.Encode(config); err != nil {
		return nil, err
	}
	if err := os.WriteFile(cfgPath, buf.Bytes(), os.ModePerm); err != nil {
		return nil, err
	}

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
	OtypeDatabase
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

// ARCH: test this instead the Unmarshal? Or both?
func UIDFromHex(data []byte) (UID, error) {
	var uid UID
	unhexed := make([]byte, 9)
	dec, err := hex.Decode(unhexed, data)
	if err != nil {
		return uid, err
	}
	if dec != len(unhexed) {
		return uid, errors.New("failed to decode UID")
	}

	uid.Otype = ObjectType(unhexed[0])
	uid.oid = binary.LittleEndian.Uint64(unhexed[1:9])
	return uid, nil
}

// UnmarshalJSON satisfies the Unmarshaler interface
// (we need a pointer here, because we'll be writing to it)
func (uid *UID) UnmarshalJSON(data []byte) error {
	if len(data) != 20 {
		return errors.New("unexpected byte array used for UIDs")
	}
	id, err := UIDFromHex(data[1:19]) // strip quotes first
	if err != nil {
		return err
	}
	*uid = id

	return nil
}

// Stripe only contains metadata about a given stripe, it has to be loaded
// separately to obtain actual data
type Stripe struct {
	Id      UID      `json:"id"`
	Length  int      `json:"length"`
	Offsets []uint32 `json:"offsets"`
}

// Dataset contains metadata for a given dataset, which at this point means a table
type Dataset struct {
	ID   UID    `json:"id"`
	Name string `json:"name"`
	// ARCH: move the next three to a a `Meta` struct?
	Created int64 `json:"created_timestamp"`
	NRows   int64 `json:"nrows"`
	// ARCH: note that we'd ideally get this as the uncompressed size... might be tricky to get
	SizeRaw    int64 `json:"size_raw"`
	SizeOnDisk int64 `json:"size_on_disk"`

	Schema column.TableSchema `json:"schema"`
	// TODO/OPTIM: we need the following for manifests, but it's unnecessary for writing in our
	// web requests - remove it from there
	Stripes []Stripe `json:"stripes"`
}

// DatasetIdentifier contains fields needed for a dataset/version lookup
type DatasetIdentifier struct {
	Name    string `json:"name"`
	Version UID    `json:"id"`
	// Latest can be used to avoid using Version (e.g. if it's unknown)
	Latest bool `json:"latest"`
}

func (did DatasetIdentifier) String() string {
	if did.Latest {
		return did.Name
	}
	return fmt.Sprintf("%s@v%s", did.Name, did.Version)
}

// NewDataset creates a new empty dataset
func NewDataset() *Dataset {
	// we need to use a high resolution timer, because subsequent dataset creation need to have a timer
	// that advanced between these actions
	// ARCH: this might be an issue in Windows, where the resolution is low?
	return &Dataset{ID: newUID(OtypeDataset), Created: time.Now().UnixNano()}
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
func (db *Database) GetDataset(did *DatasetIdentifier) (*Dataset, error) {
	var found *Dataset
	for _, dataset := range db.Datasets {
		if dataset.Name != did.Name {
			continue
		}
		if did.Latest && (found == nil || dataset.Created > found.Created) {
			found = dataset
		}
		if !did.Latest && dataset.ID == did.Version {
			return dataset, nil
		}
	}
	if found == nil {
		return nil, fmt.Errorf("dataset %v not found: %w", did.Name, errDatasetNotFound)
	}
	return found, nil
}

// AddDataset adds a Dataset to a Database
// this is a pretty rare event, so we don't expect much contention
// it's just to avoid some issues when marshaling the object around in the API etc.
func (db *Database) AddDataset(ds *Dataset) error {
	db.Lock()
	db.Datasets = append(db.Datasets, ds)
	db.Unlock()

	fn := db.manifestPath(ds)
	// only write the manifest if it doesn't exist already
	// OPTIM: consider adding a boolean arg here to avoid os.Stat on all
	// manifests upon startup (that's where it gets triggered the most)
	if _, err := os.Stat(fn); err == nil {
		return nil
	}
	f, err := os.Create(fn)
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
