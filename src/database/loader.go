package database

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/csv"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"sort"
	"strings"

	"github.com/golang/snappy"
	"github.com/kokes/smda/src/column"
)

// ARCH: reintroduce versioning (and consider how it plays along with manifest files)
// const (
// 	stripeOnDiskFormatVersion = 1
// )

// var errIncompatibleOnDiskFormat = errors.New("cannot open data stripes with a version different from the one supported")

var errIncorrectChecksum = errors.New("could not validate data on disk: incorrect checksum")
var errInvalidloadSettings = errors.New("expecting load settings for a rawLoader, got nil")
var errInvalidOffsetData = errors.New("invalid offset data")
var errSchemaMismatch = errors.New("dataset does not conform to the schema provided")
var errNoMapData = errors.New("cannot load data from a map with no data")
var errLengthMismatch = errors.New("column length mismatch")
var errCannotWriteCompression = errors.New("cannot write data compressed by this compression")

// LoadSampleData reads all CSVs from a given directory and loads them up into the database
// using default settings
func (db *Database) LoadSampleData(sampleDir fs.FS) error {
	// walking would be more efficient, but it should not matter here
	files, err := fs.Glob(sampleDir, "*")
	if err != nil {
		return fmt.Errorf("could not load samples: %w", err)
	}
	for _, file := range files {
		f, err := sampleDir.Open(file)
		if err != nil {
			return err
		}
		ds, err := db.LoadDatasetFromReaderAuto(f)
		if err != nil {
			return err
		}
		ds.Name = file
		if err := db.AddDataset(ds); err != nil {
			return err
		}
	}
	return nil
}

// CacheIncomingFile saves data from a given reader to a file
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

// ARCH: we might want to separate out there row reader thingies into a separate file,
// because this loader.go file is more about high level operations (read dataset, read into stripe etc.)
type loadSettings struct {
	// ARCH: consider the following
	// encoding
	// hasHeader
	// discardExtraColumns
	// allowFewerColumns
	cleanupColumns   bool
	readCompression  compression
	delimiter        delimiter
	schema           column.TableSchema
	writeCompression compression
}

type RowReader interface {
	// ARCH: consider making it a ([]byte, error) as soon as we
	//       1) have csv.NewReader with []byte support
	// 		 2) have strconv.ParseXXX with []byte support (will come with generics?)
	ReadRow() ([]string, error)
}

// NewRowReader creates a new RowReader based on loadSettings passed in - e.g. if there's
// a delimiter specified, it will likely create a csvReader etc.
func NewRowReader(r io.Reader, settings *loadSettings) (RowReader, error) {
	if settings == nil {
		return nil, errInvalidloadSettings
	}
	ur, err := readCompressed(r, settings.readCompression)
	if err != nil {
		return nil, err
	}
	bl, err := skipBom(ur)
	if err != nil {
		return nil, err
	}
	if settings.delimiter == delimiterTab {
		return newTSVReader(bl), nil
	}

	return newCSVReader(bl, settings)
}

type csvReader struct {
	cr *csv.Reader
}

func newCSVReader(r io.Reader, settings *loadSettings) (*csvReader, error) {
	cr := csv.NewReader(r)
	cr.ReuseRecord = true
	if settings.delimiter != delimiterNone {
		// we purposefully chose a single byte instead of a rune as a delimiter
		cr.Comma = rune(settings.delimiter)
	}

	return &csvReader{cr: cr}, nil
}

func (csvr *csvReader) ReadRow() ([]string, error) {
	row, err := csvr.cr.Read()
	// we don't want to trigger the internal ErrFieldCount,
	// we will handle column counts ourselves
	// but we'll still return EOFs for the consumer to handle
	if err != nil && err != csv.ErrFieldCount {
		return nil, err
	}
	return row, nil
}

type tsvReader struct {
	scanner *bufio.Scanner
}

func newTSVReader(r io.Reader) *tsvReader {
	scanner := bufio.NewScanner(r)
	return &tsvReader{scanner: scanner}
}

func (tsvr *tsvReader) ReadRow() ([]string, error) {
	if ok := tsvr.scanner.Scan(); !ok {
		err := tsvr.scanner.Err()
		if err == nil {
			return nil, io.EOF
		}

		return nil, err
	}
	return strings.Split(tsvr.scanner.Text(), "\t"), nil
}

var bomBytes []byte = []byte{0xEF, 0xBB, 0xBF}

// the question is if this should be a part of automatic inference or if it should
// be run on all files received (current solution) - the benefit is that you don't
// have to specify if your file is BOM-prefixed - which is something people don't
// tend to care about or know
func skipBom(r io.Reader) (io.Reader, error) {
	first := make([]byte, 3)
	n, err := r.Read(first)
	if err != nil {
		return nil, err
	}
	if bytes.Equal(first, bomBytes) {
		return r, nil
	}
	return io.MultiReader(bytes.NewReader(first[:n]), r), nil
}

type stripeData struct {
	meta    Stripe
	columns []column.Chunk
}

func newDataStripe() *stripeData {
	return &stripeData{
		meta: Stripe{Id: newUID(OtypeStripe)},
	}
}

func writeCompressed(w io.Writer, ctype compression) (io.WriteCloser, error) {
	switch ctype {
	case compressionGzip:
		return gzip.NewWriter(w), nil
	case compressionSnappy:
		return snappy.NewBufferedWriter(w), nil
	}
	// TODO: lz4, zstd
	return nil, fmt.Errorf("%w: %v", errCannotWriteCompression, ctype)
}

// pack data into a file and return their offsets, which will be stored in a manifest file
func (ds *stripeData) writeToWriter(w io.Writer, ctype compression) (nbytes int64, offsets []uint32, err error) {
	totalOffset := uint32(0)
	offsets = make([]uint32, 0, 1+len(ds.columns))
	offsets = append(offsets, 0)
	buf := new(bytes.Buffer)
	for _, column := range ds.columns {
		// OPTIM: we used to marshal into byte slices, so that we could checksum our data,
		// which can be done by writing to intermediate io.Writers instead, as shown here,
		// but we'd like to eliminate the buffer entirely and write into the underlying writer,
		// perhaps using io.MultiWriter, but that would mean placing the checksum AFTER the column
		// THOUGH PERHAPS we could just eliminate the checksum entirely and put it in our manifest file
		// will that help us with reads though? We will still have to load the whole chunk to checksum it
		if err := buf.WriteByte(byte(ctype)); err != nil {
			return 0, nil, err
		}
		if ctype == compressionNone {
			if _, err := column.WriteTo(buf); err != nil {
				return 0, nil, err
			}
		} else {
			cw, err := writeCompressed(buf, ctype)
			if err != nil {
				return 0, nil, err
			}
			if _, err := column.WriteTo(cw); err != nil {
				// TODO: are we leaking resources by not closing the writer here?
				return 0, nil, err
			}
			if err := cw.Close(); err != nil {
				return 0, nil, err
			}
		}

		nw := buf.Len()
		checksum := crc32.ChecksumIEEE(buf.Bytes())
		if err := binary.Write(w, binary.LittleEndian, checksum); err != nil {
			return 0, nil, err
		}
		if _, err := io.Copy(w, buf); err != nil {
			return 0, nil, err
		}
		totalOffset += 4 + uint32(nw) // checksum + byte slice length
		offsets = append(offsets, totalOffset)
		buf.Reset()
	}

	return int64(totalOffset), offsets, nil
}

func (db *Database) writeStripeToFile(ds *Dataset, stripe *stripeData, ctype compression) (int64, error) {
	if err := os.MkdirAll(db.DatasetPath(ds), os.ModePerm); err != nil {
		return 0, err
	}

	f, err := os.Create(db.stripePath(ds, stripe.meta))
	if err != nil {
		return 0, err
	}
	defer f.Close()
	bw := bufio.NewWriter(f)
	defer bw.Flush()

	nbytes, offsets, err := stripe.writeToWriter(bw, ctype)
	if err != nil {
		return 0, err
	}
	// ARCH: we're "injecting" offsets into a passed-in stripeData pointer,
	// should we return this instead and let the caller work with it?
	stripe.meta.Offsets = offsets
	return nbytes, nil
}

// readIntoStripe reads data from a source file and saves them into a stripe
// maybe these two arguments can be embedded into rl.settings?
func newStripeFromReader(rr RowReader, schema column.TableSchema, maxRows, maxBytes int) (*stripeData, error) {
	ds := newDataStripe()

	// given a schema, initialise a data stripe
	ds.columns = make([]column.Chunk, 0, len(schema))
	for _, col := range schema {
		ds.columns = append(ds.columns, column.NewChunkFromSchema(col))
	}

	// now let's finally load some data
	var bytesLoaded int
	for {
		row, err := rr.ReadRow()
		if err != nil {
			if err == io.EOF {
				return ds, err
			}
			return nil, err
		}
		for j, val := range row {
			bytesLoaded += len(val)
			// OPTIM: here's where all the strconv byte/string copies begin
			// or it really began in yieldRow
			// https://github.com/golang/go/issues/42429
			if err := ds.columns[j].AddValue(val); err != nil {
				return nil, fmt.Errorf("failed to populate column %v: %w", schema[j].Name, err)
			}
		}
		ds.meta.Length++

		if ds.meta.Length >= maxRows || bytesLoaded >= maxBytes {
			break
		}
	}
	return ds, nil
}

type StripeReader struct {
	f *os.File
	// seeking is slow, so keep position manually is a big win
	// if we read columns sequentially, we don't need to seek at all
	pos     int
	offsets []uint32
	schema  column.TableSchema
	buffer  *bytes.Buffer
}

// OPTIM: pass in a bytes buffer to reuse it?
func NewStripeReader(db *Database, ds *Dataset, stripe Stripe) (*StripeReader, error) {
	f, err := os.Open(db.stripePath(ds, stripe))
	if err != nil {
		return nil, err
	}

	return &StripeReader{
		f:       f,
		offsets: stripe.Offsets,
		schema:  ds.Schema,
		buffer:  new(bytes.Buffer),
	}, nil
}

func (sr *StripeReader) Close() error {
	return sr.f.Close()
}

func (sr *StripeReader) ReadColumn(nthColumn int) (column.Chunk, error) {
	offsetStart, offsetEnd := sr.offsets[nthColumn], sr.offsets[nthColumn+1]
	length := int(offsetEnd) - int(offsetStart)
	if length < 5 {
		return nil, errInvalidOffsetData
	}

	sr.buffer.Reset()
	sr.buffer.Grow(length)

	if sr.pos != int(offsetStart) {
		if _, err := sr.f.Seek(int64(offsetStart), io.SeekStart); err != nil {
			return nil, err
		}
		sr.pos = int(offsetStart)
	}
	if _, err := io.CopyN(sr.buffer, sr.f, int64(length)); err != nil {
		return nil, err
	}
	sr.pos += length

	raw := sr.buffer.Bytes()
	// IEEE CRC32 is in the first four bytes of this slice
	checksumExpected := binary.LittleEndian.Uint32(raw[:4])
	checksumGot := crc32.ChecksumIEEE(raw[4:])
	if checksumExpected != checksumGot {
		return nil, errIncorrectChecksum
	}
	ctype := compression(raw[4])

	br := bytes.NewReader(raw[5:])
	cr, err := readCompressed(br, ctype)
	if err != nil {
		return nil, err
	}
	return column.Deserialize(cr, sr.schema[nthColumn].Dtype)
}

// OPTIM: perhaps reorder the column requests, so that they are contiguous, or at least in order
//        also add a benchmark that reads columns in reverse and see if we get any benefits from this
func (db *Database) ReadColumnsFromStripeByNames(ds *Dataset, stripe Stripe, columns []string) (map[string]column.Chunk, error) {
	cols := make(map[string]column.Chunk, len(columns))
	sr, err := NewStripeReader(db, ds, stripe)
	if err != nil {
		return nil, err
	}
	defer sr.Close()
	for _, column := range columns {
		// we allow for duplicates in `columns`, so just skip those
		if _, ok := cols[column]; ok {
			continue
		}
		idx, _, err := ds.Schema.LocateColumn(column)
		if err != nil {
			return nil, err
		}
		// ARCH: consider ReadColumnByName to avoid the LocateColumn call above (and hide it in this method)
		col, err := sr.ReadColumn(idx)
		if err != nil {
			return nil, err
		}
		cols[column] = col
	}
	return cols, nil
}

func validateHeaderAgainstSchema(header []string, schema column.TableSchema) error {
	if len(header) != len(schema) {
		return errSchemaMismatch
	}

	for j, el := range header {
		if el != schema[j].Name && strings.TrimSpace(el) != schema[j].Name {
			return errSchemaMismatch
		}
	}
	return nil
}

// This is how data gets in! This is the main entrypoint
func (db *Database) loadDatasetFromReader(r io.Reader, settings *loadSettings) (*Dataset, error) {
	dataset := NewDataset()
	if settings.schema == nil {
		return nil, errors.New("cannot load data without a schema")
	}
	rr, err := NewRowReader(r, settings)
	if err != nil {
		return nil, err
	}
	// at this point we're checking all headers, but once we allow for custom schemas (e.g. renaming columns, custom type
	// declarations etc.), we'll want to have an option that skips this verification
	header, err := rr.ReadRow()
	if err != nil {
		return nil, err
	}
	if settings.cleanupColumns {
		header = cleanupColumns(header)
	}
	if err := validateHeaderAgainstSchema(header, settings.schema); err != nil {
		return nil, err
	}

	stripes := make([]Stripe, 0)
	for {
		// ARCH: this err handling is a bit clunky - can we perhaps not return io.EOF upstream? It doesn't tell us anything here...
		ds, loadingErr := newStripeFromReader(rr, settings.schema, db.Config.MaxRowsPerStripe, db.Config.MaxBytesPerStripe)
		if loadingErr != nil && loadingErr != io.EOF {
			return nil, loadingErr
		}
		dataset.NRows += int64(ds.meta.Length)
		// we started reading this stripe just as we were at the end of a file - so we only get an EOF
		// and no data
		if loadingErr == io.EOF && ds.meta.Length == 0 {
			break
		}
		// ARCH: this could possibly happen
		if ds.meta.Length == 0 {
			return nil, errors.New("no data loaded")
		}

		nbytes, err := db.writeStripeToFile(dataset, ds, settings.writeCompression)
		if err != nil {
			return nil, err
		}
		dataset.SizeOnDisk += nbytes

		stripes = append(stripes, ds.meta)

		if loadingErr == io.EOF {
			break
		}
	}

	dataset.Schema = settings.schema
	dataset.Stripes = stripes
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

// LoadDatasetFromReaderAuto loads data from a reader and returns a Dataset
func (db *Database) LoadDatasetFromReaderAuto(r io.Reader) (*Dataset, error) {
	f, err := os.CreateTemp("", "")
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

	// fallback to comma delimited files (eeek?)
	if dlim == delimiterNone {
		dlim = delimiterComma
	}

	ls := &loadSettings{
		readCompression: ctype,
		delimiter:       dlim,
		cleanupColumns:  true,
		// ARCH: we only set write compression in *Auto calls
		// TODO/OPTIM: make this configurable and optimised
		// TODO: make benchmarks compression aware (test for each compression? Or just for uncompressed?)
		writeCompression: compressionSnappy,
	}

	schema, err := inferTypes(path, ls)
	if err != nil {
		return nil, err
	}
	ls.schema = schema

	return db.loadDatasetFromLocalFile(path, ls)
}

// LoadDatasetFromMap allows for an easy setup of a new dataset, mostly useful for tests
// Converts this map into an in-memory CSV file and passes it to our usual routines
// OPTIM: the underlying call (LoadDatasetFromReaderAuto) caches this raw data on disk, may be unecessary
func (db *Database) LoadDatasetFromMap(data map[string][]string) (*Dataset, error) {
	if len(data) == 0 {
		return nil, errNoMapData
	}
	columns := make([]string, 0, len(data))
	for key := range data {
		columns = append(columns, key)
	}
	sort.Strings(columns) // to make it deterministic
	colLength := len(data[columns[0]])

	for _, col := range columns {
		vals := data[col]
		if len(vals) != colLength {
			return nil, fmt.Errorf("length mismatch in column %v: %w", col, errLengthMismatch)
		}
	}
	bf := new(bytes.Buffer)
	cw := csv.NewWriter(bf)
	if err := cw.Write(columns); err != nil {
		return nil, err
	}
	row := make([]string, len(columns))
	for j := 0; j < colLength; j++ {
		for cn := 0; cn < len(columns); cn++ {
			row[cn] = data[columns[cn]][j]
		}
		if err := cw.Write(row); err != nil {
			return nil, err
		}
	}
	cw.Flush()

	return db.LoadDatasetFromReaderAuto(bf)
}
