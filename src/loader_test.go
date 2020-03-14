package smda

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestAutoInferenceInLoading(t *testing.T) {
	tests := []struct {
		contents    string
		columns     []string
		compression compression
		filename    string
	}{
		{"foo,bar,baz\n1,2,3", []string{"foo", "bar", "baz"}, compressionNone, "foo.csv"},
		{"foo;bar;baz\n1;2;3", []string{"foo", "bar", "baz"}, compressionNone, "foo.tsv"},
		{"foo,bar,baz\n1,2,3", []string{"foo", "bar", "baz"}, compressionGzip, "foo.csv.gz"},
		{"foo;bar;baz\n1;2;3", []string{"foo", "bar", "baz"}, compressionGzip, "foo.bin"}, // filename need not indicate compression
	}

	tdir, err := ioutil.TempDir("", "test_compression")
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range tests {
		d, err := NewDatabaseTemp()
		if err != nil {
			t.Fatal(err)
		}
		bf := new(bytes.Buffer)
		switch test.compression {
		case compressionNone:
			bf.Write([]byte(test.contents))
		case compressionGzip:
			gw := gzip.NewWriter(bf)
			gw.Write([]byte(test.contents))
			gw.Close()
		default:
			t.Fatalf("unsupported compression for writing: %v", test.compression)
		}

		// first try from a reader
		ds, err := d.loadDatasetFromReaderAuto(bytes.NewReader(bf.Bytes()))
		if err != nil {
			t.Fatal(err)
		}
		dsCols := make([]string, 0)
		for _, col := range ds.Schema {
			dsCols = append(dsCols, col.Name)
		}
		if !reflect.DeepEqual(dsCols, test.columns) {
			t.Errorf("expecting columns to be %v, got %v", test.columns, dsCols)
		}

		// but also from a file
		tfn := filepath.Join(tdir, test.filename)
		if err := ioutil.WriteFile(tfn, bf.Bytes(), os.ModePerm); err != nil {
			t.Fatal(err)
		}
		ds, err = d.loadDatasetFromLocalFileAuto(tfn)
		if err != nil {
			t.Fatal(err)
		}
		dsCols = make([]string, 0)
		for _, col := range ds.Schema {
			dsCols = append(dsCols, col.Name)
		}
		if !reflect.DeepEqual(dsCols, test.columns) {
			t.Errorf("expecting columns to be %v, got %v", test.columns, dsCols)
		}
	}
}

func TestReadingFromStripes(t *testing.T) {
	db, err := NewDatabaseTemp()
	if err != nil {
		t.Fatal(err)
	}
	buf := strings.NewReader("foo,bar,baz\n1,12,13\n1444,112,13")

	ds, err := db.loadDatasetFromReaderAuto(buf)
	if err != nil {
		t.Fatal(err)
	}
	col, err := db.readColumnFromStripe(ds, ds.Stripes[0], 0)
	if err != nil {
		t.Fatal(err)
	}
	cols := col.(*columnInts)
	if cols.length != 2 {
		t.Fatalf("expecting the length to be %v, got %v", 2, cols.length)
	}
}

// columnSchema JSON marshaling (because of dtypes)
// func (db *Database) LoadSampleData(path string) error {
// func cacheIncomingFile(r io.Reader, path string) error {
// func (db *Database) LoadRawDataset(r io.Reader) (*Dataset, error) {
// func newRawLoader(r io.Reader, settings loadSettings) (*rawLoader, error) {
// func newDataStripe() *dataStripe {
// func (ds *dataStripe) writeToWriter(w io.Writer) error {
// func (ds *dataStripe) writeToFile(rootDir, datasetID string) error {
// func (rl *rawLoader) ReadIntoStripe(maxRows, maxBytes int) (*dataStripe, error) {
// 		// perhaps wrap this in an init function that returns a schema, so that we have less cruft here
// func (db *Database) castDataset(ds *Dataset, newSchema []columnSchema) (*Dataset, error) {
// func (db *Database) readColumnFromStripe(ds *Dataset, stripeID uid, nthColumn int) (typedColumn, error) {
// func (db *Database) loadDatasetFromReader(r io.Reader, settings loadSettings) (*Dataset, error) {
// func (db *Database) loadDatasetFromLocalFile(path string, settings loadSettings) (*Dataset, error) {
// func (db *Database) loadDatasetFromReaderAuto(r io.Reader) (*Dataset, error) {
// func (db *Database) loadDatasetFromLocalFileAuto(path string) (*Dataset, error) {
