package database

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

const (
	stripeOnDiskFormatVersion = 1
)

var errIncorrectChecksum = errors.New("could not validate data on disk: incorrect checksum")
var errIncompatibleOnDiskFormat = errors.New("cannot open data stripes with a version different from the one supported")
var errInvalidLoadSettings = errors.New("expecting load settings for a rawLoader, got nil")
var errInvalidOffsetData = errors.New("invalid offset data")
var errSchemaMismatch = errors.New("dataset does not conform to the schema provided")

// LoadSampleData reads all CSVs from a given directory and loads them up into the database
// using default settings
// TODO: this will fall into the go-bindata packing issue (also includes webserver's static files)
func (db *Database) LoadSampleData(path string) error {
	// walking would be more efficient, but it should not matter here
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return fmt.Errorf("could not load samples: %w", err)
	}
	for _, file := range files {
		ffn := filepath.Join(path, file.Name())
		_, err := db.loadDatasetFromLocalFileAuto(ffn)
		if err != nil {
			return err
		}
	}
	return nil
}

func CacheIncomingFile(r io.Reader, path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	bw := bufio.NewWriter(f)
	defer bw.Flush()
	_, err = io.Copy(bw, r)
	if err != nil {
		return err
	}
	return nil
}

type loadSettings struct {
	// encoding
	compression compression
	delimiter   delimiter
	// hasHeader
	schema TableSchema
	// discardExtraColumns
	// allowFewerColumns
}

// this might be confuse with `LoadRawDataset`, but they do very different things
// the former caches stuff, this actually loads data from raw files
type rawLoader struct {
	settings *loadSettings
	// we don't need this CSV pkg to be here, we can put an interface here (we only use Read() ([]string, error))
	cr *csv.Reader
}

func newRawLoader(r io.Reader, settings *loadSettings) (*rawLoader, error) {
	if settings == nil {
		return nil, errInvalidLoadSettings
	}
	ur, err := wrapCompressed(r, settings.compression)
	if err != nil {
		return nil, err
	}
	cr := csv.NewReader(ur)
	cr.ReuseRecord = true
	if settings.delimiter != delimiterNone {
		// we purposefully chose a single byte instead of a rune as a delimiter
		cr.Comma = rune(settings.delimiter)
	}

	return &rawLoader{settings: settings, cr: cr}, nil
}

type dataStripe struct {
	id      UID
	columns []TypedColumn // pointers instead?
}

func newDataStripe() *dataStripe {
	return &dataStripe{
		id: newUID(OtypeStripe),
	}
}

// the layout is: [column][column][column][offsets]
// where [column] is a byte-representation of a column (or its chunk, if there are multiple stripes)
// and [offsets] is an array of uint64 offsets of individual columns (start + end, so ncolumns + 1 uints)
// since we know how many columns are in this file (from the dataset metadata), we first
// need to read that many bytes off the end of the file and then offset to whichever column we want
func (ds *dataStripe) writeToWriter(w io.Writer) error {
	totalOffset := uint64(0)
	offsets := make([]uint64, 0, len(ds.columns))
	for _, column := range ds.columns {
		// OPTIM: we used to write column data directly into the underlying writer, but we introduced
		// a method that returns a slice, so that we can checksum it - this increased our allocations, so
		// we may want to reconsider this and perhaps checksum on the fly, feed in a bytes buffer or something
		b, err := column.MarshalBinary()
		if err != nil {
			return err
		}
		checksum := crc32.ChecksumIEEE(b)
		if err := binary.Write(w, binary.LittleEndian, checksum); err != nil {
			return err
		}
		n, err := w.Write(b)
		if n != len(b) {
			return fmt.Errorf("failed to serialise a column, expecting to write %v bytes, wrote %b", len(b), n)
		}
		if err != nil {
			return err
		}
		totalOffset += uint64(len(b)) + 4 // byte slice length + checksum
		offsets = append(offsets, totalOffset)
	}

	header := new(bytes.Buffer)
	version := uint16(stripeOnDiskFormatVersion)
	if err := binary.Write(header, binary.LittleEndian, version); err != nil {
		return err
	}
	if err := binary.Write(header, binary.LittleEndian, offsets); err != nil {
		return err
	}
	headerLength := uint32(header.Len())
	if err := binary.Write(header, binary.LittleEndian, headerLength); err != nil {
		return err
	}
	if _, err := io.Copy(w, header); err != nil {
		return err
	}
	return nil
}

func (db *Database) writeStripeToFile(ds *Dataset, stripe *dataStripe) error {
	if err := os.MkdirAll(db.DatasetPath(ds), os.ModePerm); err != nil {
		return err
	}

	f, err := os.Create(db.stripePath(ds, stripe.id))
	if err != nil {
		return err
	}
	defer f.Close()
	bw := bufio.NewWriter(f)
	defer bw.Flush()

	return stripe.writeToWriter(bw)
}

func (rl *rawLoader) yieldRow() ([]string, error) {
	row, err := rl.cr.Read()
	// we don't want to trigger the internal ErrFieldCount,
	// we will handle column counts ourselves
	// but we'll still return EOFs for the consumer to handle
	if err != nil && err != csv.ErrFieldCount {
		return nil, err
	}
	return row, nil
}

// maybe these two arguments can be embedded into rl.settings?
func (rl *rawLoader) ReadIntoStripe(maxRows, maxBytes int) (*dataStripe, error) {
	ds := newDataStripe()
	// if no schema is set, read the header and set it yourself (to be all non-nullable strings)
	if rl.settings.schema == nil {
		hd, err := rl.yieldRow()
		if err != nil {
			// this can trigger an EOF, which would signal that the source if empty
			return nil, err
		}
		// perhaps wrap this in an init function that returns a schema, so that we have less cruft here
		rl.settings.schema = make(TableSchema, 0, len(hd))
		for _, val := range hd {
			rl.settings.schema = append(rl.settings.schema, ColumnSchema{
				Name:     val,
				Dtype:    DtypeString,
				Nullable: false,
			})
		}
	}

	// given a schema, initialise a data stripe
	ds.columns = make([]TypedColumn, 0, len(rl.settings.schema))
	for _, col := range rl.settings.schema {
		ds.columns = append(ds.columns, NewTypedColumnFromSchema(col))
	}

	// now let's finally load some data
	var bytesLoaded int
	var rowsLoaded int
	for {
		row, err := rl.yieldRow()
		if err != nil {
			if err == io.EOF {
				return ds, err
			}
			return nil, err
		}
		for j, val := range row {
			bytesLoaded += len(val)
			if err := ds.columns[j].AddValue(val); err != nil {
				return nil, fmt.Errorf("failed to populate column %v: %w", rl.settings.schema[j].Name, err)
			}
		}
		rowsLoaded++

		if rowsLoaded >= maxRows || bytesLoaded >= maxBytes {
			break
		}
	}
	return ds, nil
}

// we could probably make use of a "stripeReader", which would only open the file once
// by using this, we will open and close the file every time we want a column
// OPTIM: this does not buffer any reads... but it only reads things thrice, so it shouldn't matter, right?
func (db *Database) ReadColumnFromStripe(ds *Dataset, stripeID UID, nthColumn int) (TypedColumn, error) {
	f, err := os.Open(db.stripePath(ds, stripeID))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if _, err := f.Seek(-4, os.SEEK_END); err != nil {
		return nil, err
	}
	var headerSize uint32
	if err := binary.Read(f, binary.LittleEndian, &headerSize); err != nil {
		return nil, err
	}
	if _, err := f.Seek(-4-int64(headerSize), os.SEEK_END); err != nil {
		return nil, err
	}

	header := make([]byte, headerSize)
	if _, err := io.ReadFull(f, header); err != nil {
		return nil, err
	}
	version := binary.LittleEndian.Uint16(header[:2])
	if version != stripeOnDiskFormatVersion {
		return nil, errIncompatibleOnDiskFormat
	}

	ncolumns := len(ds.Schema)
	offsets := make([]uint64, 0, ncolumns)
	offsets = append(offsets, 0)

	// OPTIM: technically, we don't need to load all the offsets (unless we want to cache them)
	for j := 0; j < ncolumns; j++ {
		newOffset := binary.LittleEndian.Uint64(header[2+j*8 : 2+(j+1)*8]) // TODO: test the length some place above
		offsets = append(offsets, newOffset)
	}
	offsetStart, offsetEnd := offsets[nthColumn], offsets[nthColumn+1]
	// a non-complete guard against bit rot and other nasties
	// only allow sequential offsets and only offer 4 gigs per column chunk
	// it should also have at least four bytes
	if offsetEnd < offsetStart || offsetEnd-offsetStart > 1<<32 || offsetEnd-offsetStart < 4 {
		return nil, errInvalidOffsetData
	}

	buf := make([]byte, offsetEnd-offsetStart)
	n, err := f.ReadAt(buf, int64(offsetStart))
	if err != nil {
		return nil, err
	}
	if n != len(buf) {
		return nil, fmt.Errorf("expected to read %v bytes, but only got %v", len(buf), n)
	}

	// IEEE CRC32 is in the first four bytes of this slice
	checksumExpected := binary.LittleEndian.Uint32(buf[:4])
	checksumGot := crc32.ChecksumIEEE(buf[4:])
	if checksumExpected != checksumGot {
		return nil, errIncorrectChecksum
	}

	br := bytes.NewReader(buf[4:])
	return deserializeColumn(br, ds.Schema[nthColumn].Dtype)
}

func validateHeaderAgainstSchema(header []string, schema TableSchema) error {
	if len(header) != len(schema) {
		return errSchemaMismatch
	}

	for j, el := range header {
		if el != schema[j].Name {
			return errSchemaMismatch
		}
	}
	return nil
}

// This is how data gets in! This is the main entrypoint
// TODO: log dependency on the raw dataset somehow? lineage?
// TODO: we have quite an inconsistency here - LoadDatasetFromReaderAuto caches incoming data and loads them then,
// this reads it without any caching (at the same time... if we cache it here, we'll be caching it twice,
// because we load it from our Auto methods - we'd have to call the file reader here [should be fine])
func (db *Database) loadDatasetFromReader(r io.Reader, settings *loadSettings) (*Dataset, error) {
	dataset := NewDataset()
	rl, err := newRawLoader(r, settings)
	if err != nil {
		return nil, err
	}
	if rl.settings.schema == nil {
		return nil, errors.New("cannot load data without a schema")
	}
	// at this point we're checking all headers, but once we allow for custom schemas (e.g. renaming columns, custom type
	// declarations etc.), we'll want to have an option that skips this verification
	header, err := rl.yieldRow()
	if err != nil {
		return nil, err
	}
	if err := validateHeaderAgainstSchema(header, rl.settings.schema); err != nil {
		return nil, err
	}

	stripes := make([]UID, 0)
	for {
		ds, loadingErr := rl.ReadIntoStripe(db.Config.MaxRowsPerStripe, db.Config.MaxBytesPerStripe)
		if loadingErr != nil && loadingErr != io.EOF {
			return nil, loadingErr
		}
		stripes = append(stripes, ds.id)

		if err := db.writeStripeToFile(dataset, ds); err != nil {
			return nil, err
		}

		if loadingErr == io.EOF {
			break
		}
	}

	dataset.Schema = rl.settings.schema
	dataset.Stripes = stripes
	db.AddDataset(dataset)
	return dataset, nil
}

// convenience wrapper
func (db *Database) loadDatasetFromLocalFile(path string, settings *loadSettings) (*Dataset, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return db.loadDatasetFromReader(f, settings)
}

func (db *Database) LoadDatasetFromReaderAuto(r io.Reader) (*Dataset, error) {
	f, err := ioutil.TempFile("", "")
	if err != nil {
		return nil, err
	}
	defer os.Remove(f.Name())
	if err := CacheIncomingFile(r, f.Name()); err != nil {
		return nil, err
	}

	return db.loadDatasetFromLocalFileAuto(f.Name())
}

func (db *Database) loadDatasetFromLocalFileAuto(path string) (*Dataset, error) {
	ctype, dlim, err := inferCompressionAndDelimiter(path)
	if err != nil {
		return nil, err
	}

	ls := &loadSettings{
		compression: ctype,
		delimiter:   dlim,
	}

	schema, err := inferTypes(path, ls)
	if err != nil {
		return nil, err
	}
	ls.schema = schema

	return db.loadDatasetFromLocalFile(path, ls)
}
