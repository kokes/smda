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
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()

	buf := strings.NewReader("foo,bar,baz\n1,true,1.23\n1444,,1e8")

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
		t.Errorf("expecting the length to be %v, got %v", 2, cols.length)
	}

	col, err = db.readColumnFromStripe(ds, ds.Stripes[0], 1)
	if err != nil {
		t.Fatal(err)
	}
	colb := col.(*columnBools)
	if colb.length != 2 {
		t.Errorf("expecting the length to be %v, got %v", 2, colb.length)
	}
	if !colb.nullable {
		t.Errorf("expecting the second column to be nullable")
	}

	col, err = db.readColumnFromStripe(ds, ds.Stripes[0], 2)
	if err != nil {
		t.Fatal(err)
	}
	colf := col.(*columnFloats)
	if colf.length != 2 {
		t.Errorf("expecting the length to be %v, got %v", 2, colf.length)
	}
}

// note that this measures throughput in terms of the original file size, not the size it takes on the disk
func BenchmarkReadingFromStripes(b *testing.B) {
	db, err := NewDatabaseTemp()
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()

	header := "foo,bar,baz\n"
	row := "1,true,1.23\n"
	for _, nrows := range []int{1, 100, 1000, 1000_000} {
		bName := strconv.Itoa(nrows)
		b.Run(bName, func(b *testing.B) {
			buf := new(bytes.Buffer)
			if _, err := buf.Write([]byte(header)); err != nil {
				b.Fatal(err)
			}
			for j := 0; j < nrows; j++ {
				if _, err := buf.Write([]byte(row)); err != nil {
					b.Fatal(err)
				}
			}

			b.SetBytes(int64(buf.Len()))

			ds, err := db.loadDatasetFromReaderAuto(buf)
			if err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			for j := 0; j < b.N; j++ {
				for cn := 0; cn < 3; cn++ {
					crows := 0
					for _, stripeID := range ds.Stripes {
						col, err := db.readColumnFromStripe(ds, stripeID, cn)
						if err != nil {
							b.Fatal(err)
						}
						crows += col.Len()
					}

					if crows != nrows {
						b.Errorf("expecting %v rows, got %v", nrows, crows)
					}
				}
			}
		})
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
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()

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

func TestLoadingOfRawDatasets(t *testing.T) {
	db, err := NewDatabaseTemp()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()

	data := strings.NewReader("foo,bar,baz\n1,2,3\n4,5,6")
	ds, err := db.LoadRawDataset(data)
	if err != nil {
		t.Fatal(err)
	}
	if ds.Schema != nil {
		t.Error("expecting a temp raw dataset not to have a schema")
	}
}

// if we flip any single bit in the file - apart from the checksums and version, we should get a checksum error
// TODO: guard against offset bit rot? We do get a panic due to `buf := make([]byte, offsetEnd-offsetStart)` allocating too much
func TestChecksumValidation(t *testing.T) {
	db, err := NewDatabaseTemp()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()

	buf := strings.NewReader("foo,bar,baz\n1,true,1.23\n1444,,1e8")

	ds, err := db.loadDatasetFromReaderAuto(buf)
	if err != nil {
		t.Fatal(err)
	}
	// this should work fine
	stripeID := ds.Stripes[0]
	readStripes := func() error {
		for colNum := 0; colNum < 3; colNum++ {
			_, err := db.readColumnFromStripe(ds, stripeID, colNum)
			if err != nil {
				return err
			}
		}
		return nil
	}
	if err := readStripes(); err != nil {
		t.Fatal(err)
	}
	path := db.stripePath(ds, stripeID)
	stripeData, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	mut := make([]byte, len(stripeData))
	// we don't read the first two bytes (format version)
	// and the last 32 (4 offsets, 8 bytes each)
	for j := 2; j < len(stripeData)-32; j++ {
		copy(mut, stripeData) // copy fresh data, so that we can mutate them
		for pos := 0; pos < 8; pos++ {
			if mut[j]&(1<<pos) > 0 {
				mut[j] &^= 1 << pos
			} else {
				mut[j] |= 1 << pos
			}
			if err := ioutil.WriteFile(path, mut, os.ModePerm); err != nil {
				t.Error(err)
				continue
			}
			if err := readStripes(); err != errIncorrectChecksum {
				t.Errorf("flipping bits should trigger %v, got %v instead", errIncorrectChecksum, err)
			}
		}
	}
}

// func newRawLoader(r io.Reader, settings loadSettings) (*rawLoader, error) {
// func (ds *dataStripe) writeToWriter(w io.Writer) error {
// func (ds *dataStripe) writeToFile(rootDir, datasetID string) error { -- signature has changed, it's now writeStripeToFile
// func (rl *rawLoader) ReadIntoStripe(maxRows, maxBytes int) (*dataStripe, error) {
// func (db *Database) loadDatasetFromReader(r io.Reader, settings loadSettings) (*Dataset, error) {
// func (db *Database) loadDatasetFromLocalFile(path string, settings loadSettings) (*Dataset, error) {
