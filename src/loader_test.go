package smda

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
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
		defer os.RemoveAll(d.WorkingDirectory)
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
	defer os.RemoveAll(db.WorkingDirectory)
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

func TestColumnSchemaMarshalingRoundtrips(t *testing.T) {
	cs := columnSchema{Name: "foo", Dtype: dtypeBool, Nullable: true}
	dt, err := json.Marshal(cs)
	if err != nil {
		t.Fatal(err)
	}
	var cs2 columnSchema
	if err := json.Unmarshal(dt, &cs2); err != nil {
		t.Fatal(err)
	}
}

func TestLoadingSampleData(t *testing.T) {
	db, err := NewDatabaseTemp()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(db.WorkingDirectory)

	tmpdir, err := ioutil.TempDir("", "sample_data")
	if err != nil {
		t.Fatal(err)
	}
	// prep some sample data
	fns := []string{"foo.csv", "bar.tsv", "baz.csv.gz"}
	for _, fn := range fns {
		tfn := filepath.Join(tmpdir, fn)
		var data []byte
		if strings.HasSuffix(fn, ".csv") {
			data = []byte("foo,bar,baz\n1,2,3\n3,4,5")
		} else if strings.HasSuffix(fn, ".tsv") {
			data = []byte("foo\tbar\tbaz\n1\t2\t3\n4\t5\t6")
		} else if strings.HasSuffix(fn, ".csv.gz") {
			buf := new(bytes.Buffer)
			gw := gzip.NewWriter(buf)
			if _, err := gw.Write([]byte("foo,bar,baz\n1,2,3\n3,4,5")); err != nil {
				t.Fatal(err)
			}
			gw.Close()
			data = buf.Bytes() // [:]
		} else {
			panic("misspecified test case")
		}

		if err := ioutil.WriteFile(tfn, data, os.ModePerm); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.LoadSampleData(tmpdir); err != nil {
		t.Fatal(err)
	}

	if len(db.Datasets) != len(fns) {
		t.Errorf("expecting %v datasets, got %v", len(fns), len(db.Datasets))
	}

	ecols := []string{"foo", "bar", "baz"}
	for _, ds := range db.Datasets {
		cols := make([]string, 0)
		for _, col := range ds.Schema {
			cols = append(cols, col.Name)
		}
		if !reflect.DeepEqual(cols, ecols) {
			t.Errorf("expecting each dataset to have the header of %v, got %v", ecols, cols)
		}
	}
}

func TestBasicFileCaching(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "caching")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdir)
	for _, size := range []int{0, 1000, 1000_1000} {
		buf := new(bytes.Buffer)
		for j := 0; j < size; j++ {
			if _, err := buf.Write([]byte{byte(j % 256)}); err != nil {
				t.Fatal(err)
			}
		}
		rd := bytes.NewReader(buf.Bytes())
		path := filepath.Join(tmpdir, strconv.Itoa(size))
		if err := cacheIncomingFile(rd, path); err != nil {
			t.Error(err)
			continue
		}
		contents, err := ioutil.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(contents, buf.Bytes()) {
			t.Errorf("roundtrip failed for %v bytes", size)
		}
	}
}

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
