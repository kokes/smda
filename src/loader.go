package smda

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/kokes/smda/src/csv"
)

// TODO: to be revised once we're done with testing
const (
	maxRowsPerStripe  = 5_000
	maxBytesPerStripe = 500_000
)

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

func cacheIncomingFile(r io.Reader, path string) error {
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

// LoadRawDataset loads a 'special' dataset, one which is only used for parsing, it cannot be queried
// in any way, so I question the validity of calling this 'dataset'
// we may rename this as all it does is local caching (at that point we might as well use
// cacheIncomingFile directly) - but let's keep in mind that we might need to reprocess the raw
// dataset at a later point (if schema changes, if we need to infer it again etc.)
func (db *Database) LoadRawDataset(r io.Reader) (*Dataset, error) {
	d := NewDataset()
	d.LocalFilepath = filepath.Join(db.WorkingDirectory, d.ID.String())

	if err := cacheIncomingFile(r, d.LocalFilepath); err != nil {
		return nil, err
	}
	return d, nil
}

type columnSchema struct {
	Name     string `json:"name"`
	Dtype    dtype  `json:"dtype"`
	Nullable bool   `json:"nullable"`
}

// TODO: this should probably be a pointer everywhere?
type loadSettings struct {
	// encoding
	compression compression
	delimiter   delimiter
	// hasHeader
	schema []columnSchema
	// discardExtraColumns
	// allowFewerColumns
}

// this might be confuse with `LoadRawDataset`, but they do very different things
// the former caches stuff, this actually loads data from raw files
type rawLoader struct {
	settings loadSettings
	// we don't need this CSV pkg to be here, we can put an interface here (we only use Read() ([]string, error))
	cr *csv.Reader
}

func newRawLoader(r io.Reader, settings loadSettings) (*rawLoader, error) {
	ur, err := wrapCompressed(r, settings.compression)
	if err != nil {
		return nil, err
	}
	cr := csv.NewReader(ur)
	if settings.delimiter != delimiterNone {
		// we purposefully chose a single byte instead of a rune as a delimiter
		cr.Comma = rune(settings.delimiter)
	}
	cr.ReuseRecord = true

	return &rawLoader{settings: settings, cr: cr}, nil
}

type dataStripe struct {
	id      UID
	columns []typedColumn // pointers instead?
}

func newDataStripe() *dataStripe {
	return &dataStripe{
		id: newUID(otypeStripe),
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
	offsets = append(offsets, 0)
	for _, column := range ds.columns {
		n, err := column.serializeInto(w)
		if err != nil {
			return err
		}
		totalOffset += uint64(n)
		offsets = append(offsets, totalOffset)
	}
	return binary.Write(w, binary.LittleEndian, offsets)
}

// TODO: this whole API is just wrong - we should have a `db.writestripe(ds, stripe) error`
func (ds *dataStripe) writeToFile(rootDir, datasetID string) error {
	tdir := filepath.Join(rootDir, datasetID)
	if err := os.MkdirAll(tdir, os.ModePerm); err != nil {
		return err
	}

	// TODO: d.LocalFilepath? (though we don't have access to `d` here)
	path := filepath.Join(tdir, ds.id.String())
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	bw := bufio.NewWriter(f)
	defer bw.Flush()

	return ds.writeToWriter(bw)
}

func (rl *rawLoader) ReadIntoStripe(maxRows, maxBytes int) (*dataStripe, error) {
	ds := newDataStripe()
	// if no schema is set, read the header and set it yourself (to be all non-nullable strings)
	if rl.settings.schema == nil {
		hd, err := rl.cr.Read()
		if err != nil {
			return nil, err // TODO: EOF handling?
		}
		// perhaps wrap this in an init function that returns a schema, so that we have less cruft here
		rl.settings.schema = make([]columnSchema, 0, len(hd))
		for _, val := range hd {
			rl.settings.schema = append(rl.settings.schema, columnSchema{
				Name:     string(val),
				Dtype:    dtypeString,
				Nullable: false,
			})
		}
	}

	// given a schema, initialise a data stripe
	ds.columns = make([]typedColumn, 0, len(rl.settings.schema))
	for _, col := range rl.settings.schema {
		ds.columns = append(ds.columns, newTypedColumnFromSchema(col))
	}

	// now let's finally load some data
	var bytesLoaded int
	var rowsLoaded int
	for {
		row, err := rl.cr.Read()
		// we don't want to trigger the internal ErrFieldCount,
		// we will handle column counts ourselves
		if err != nil && err != csv.ErrFieldCount {
			// I think we need to report EOFs, because that will signalise to downstream
			// that no more stripe reads will be possible
			if err == io.EOF {
				return ds, err
			}
			return nil, err
		}
		for j, val := range row {
			bytesLoaded += len(val)
			ds.columns[j].addValue(val)
		}
		rowsLoaded++

		if rowsLoaded > maxRows || bytesLoaded > maxBytes {
			break
		}
	}
	return ds, nil
}

// will cast it into the same number of stripes, so if we have vastly more (or less) efficient columns,
// the data size of these may no longer be "optimal"
func (db *Database) castDataset(ds *Dataset, newSchema []columnSchema) (*Dataset, error) {
	if len(ds.Schema) != len(newSchema) {
		return nil, errors.New("schema mismatch")
	}
	// check that the existing schema is all strings
	for _, col := range ds.Schema {
		if col.Dtype != dtypeString {
			return nil, errors.New("can only cast from string columns")
		}
	}
	newDs := NewDataset()

	newStripeIDs := make([]UID, 0, len(newSchema))
	for _, stripeID := range ds.Stripes {
		newStripe := newDataStripe()

		for colNum, schema := range newSchema {
			newCol := newTypedColumnFromSchema(schema)
			col, err := db.readColumnFromStripe(ds, stripeID, colNum)
			if err != nil {
				return nil, err
			}
			scol := col.(*columnStrings)
			for rowNum := 0; rowNum < scol.Len(); rowNum++ {
				newCol.addValue(scol.nthValue(rowNum))
			}
			newStripe.columns = append(newStripe.columns, newCol)
		}

		newStripeIDs = append(newStripeIDs, newStripe.id)

		// TODO: d.localfile
		if err := newStripe.writeToFile(db.WorkingDirectory, newDs.ID.String()); err != nil {
			return nil, err
		}
	}

	newDs.Schema = newSchema
	newDs.Stripes = newStripeIDs
	db.addDataset(newDs)
	return newDs, nil
}

// we could probably make use of a "stripeReader", which would only open the file once
// by using this, we will open and close the file every time we want a column
// OPTIM: this does not buffer any reads... but it only reads things twice, so it shouldn't matter, right?
func (db *Database) readColumnFromStripe(ds *Dataset, stripeID UID, nthColumn int) (typedColumn, error) {
	// TODO: d.LocalFilePath? (is probably not filled in here)
	path := filepath.Join(db.WorkingDirectory, ds.ID.String(), stripeID.String())
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	ncolumns := len(ds.Schema)

	offsets := make([]uint64, ncolumns+1)

	_, err = f.Seek(-int64(len(offsets)*8), io.SeekEnd)
	if err != nil {
		return nil, err
	}
	if err = binary.Read(f, binary.LittleEndian, &offsets); err != nil {
		return nil, err
	}
	offsetStart, offsetEnd := offsets[nthColumn], offsets[nthColumn+1]

	buf := make([]byte, offsetEnd-offsetStart)
	n, err := f.ReadAt(buf, int64(offsetStart))
	if err != nil {
		return nil, err
	}
	if n != len(buf) {
		return nil, fmt.Errorf("expected to read %v bytes, but only got %v", len(buf), n)
	}

	br := bytes.NewReader(buf)
	return deserializeColumn(br, ds.Schema[nthColumn].Dtype)
}

// This is how data gets in! This is the main entrypoint
// TODO: log dependency on the raw dataset somehow? lineage?
func (db *Database) loadDatasetFromReader(r io.Reader, settings loadSettings) (*Dataset, error) {
	dataset := NewDataset()
	rl, err := newRawLoader(r, settings)
	if err != nil {
		return nil, err
	}
	stripes := make([]UID, 0)
	for {
		ds, loadingErr := rl.ReadIntoStripe(maxRowsPerStripe, maxBytesPerStripe)
		if loadingErr != nil && loadingErr != io.EOF {
			return nil, loadingErr
		}
		stripes = append(stripes, ds.id)

		// TODO: shouldn't this be d.LocalFilePath?
		if err := ds.writeToFile(db.WorkingDirectory, dataset.ID.String()); err != nil {
			return nil, err
		}

		if loadingErr == io.EOF {
			break
		}
	}

	dataset.Schema = rl.settings.schema
	dataset.Stripes = stripes
	db.addDataset(dataset)
	return dataset, nil
}

// convenience wrapper
func (db *Database) loadDatasetFromLocalFile(path string, settings loadSettings) (*Dataset, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return db.loadDatasetFromReader(f, settings)
}

func (db *Database) loadDatasetFromReaderAuto(r io.Reader) (*Dataset, error) {
	header := make([]byte, 64)
	n, err := r.Read(header)
	if err != nil && err != io.EOF {
		return nil, err
	}
	header = header[:n] // we'd otherwise have null-byte padding after whatever we loaded
	ctype, err := inferCompression(header)
	if err != nil {
		return nil, err
	}
	mr := io.MultiReader(bytes.NewReader(header), r)
	uf, err := wrapCompressed(mr, ctype)

	// now read some uncompressed data to determine a delimiter
	uheader := make([]byte, 64*1024)
	n, err = uf.Read(uheader)
	if err != nil && err != io.EOF {
		return nil, err
	}
	uheader = uheader[:n]
	umr := io.MultiReader(bytes.NewReader(uheader), uf)

	dlim, err := inferDelimiter(uheader)
	if err != nil {
		return nil, err
	}

	ls := loadSettings{
		// we no longer need to let the loader know it's compressed
		// because we've already uncompressed it here to determine the delimiter
		// compression: ctype,
		delimiter: dlim,
	}

	// OPTIM: we don't need to load the whole dataset here in most cases
	// we could infer types on the raw data and then load the dataset with the correct schema
	// one thing to keep in mind: the schema from the first 1000 rows may differ from the whole file
	//  - do we fail on a load then? do we allow for runtime changes? (e.g. int -> float, not nullable -> nullable etc.)
	ds, err := db.loadDatasetFromReader(umr, ls)
	if err != nil {
		return nil, err
	}

	schema, err := db.inferTypes(ds)
	if err != nil {
		return nil, err
	}
	defer db.removeDataset(ds) // remove the string-only dataset
	// now that we have inferred a schema from strings, we have two options
	// 1) we pass this schema to our CSV reader and let all the existing code do its work
	// 2) write a new method, which will take our binary files and resave them using this new schema
	// the second option, while longer, will be much more performant
	// also, we don't have the CSV anymore - we used a reader after all
	return db.castDataset(ds, schema)
}

func (db *Database) loadDatasetFromLocalFileAuto(path string) (*Dataset, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return db.loadDatasetFromReaderAuto(f)
}
