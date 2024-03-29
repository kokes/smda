package database

import (
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/kokes/smda/src/column"
)

func TestAutoInferenceInLoading(t *testing.T) {
	tests := []struct {
		contents    string
		columns     []string
		compression compression
		filename    string
	}{
		{"foo,bar,baz\n1,2,3", []string{"foo", "bar", "baz"}, compressionNone, "foo.csv"},
		// leading/trailing whitespace in column names shouldn't matter
		{"foo ,\" bar\n\",\" baz\t\"\n1,2,3\n", []string{"foo", "bar", "baz"}, compressionNone, "foo.csv"},
		{"foo;bar;baz\n1;2;3", []string{"foo", "bar", "baz"}, compressionNone, "foo.tsv"},
		{"foo\tbar\tbaz\n1\t2\t3\n", []string{"foo", "bar", "baz"}, compressionNone, "foo.tsv"},
		// in TSVs, we can have bare quotes, because we can't have newlines etc. in fields, TSV is something a lot different from CSV
		{"foo\tbar\tba\"z\n1\t2\t3\n", []string{"foo", "bar", "ba_z"}, compressionNone, "foo.tsv"},
		{"foo,bar,baz\n1,2,3", []string{"foo", "bar", "baz"}, compressionGzip, "foo.csv.gz"},
		{"foo;bar;baz\n1;2;3", []string{"foo", "bar", "baz"}, compressionGzip, "foo.bin"},                         // filename need not indicate compression
		{"\xEF\xBB\xBFfoo;bar;baz\n1;2;3", []string{"foo", "bar", "baz"}, compressionNone, "foo_bom.csv"},         // BOM
		{"\xEF\xBB\xBF\"foo\";\"bar\";baz\n1;2;3", []string{"foo", "bar", "baz"}, compressionNone, "foo_bom.csv"}, // BOM messes with delimiter inference
		{"\"here is my very long quoted column that will get only parsed if we pass in our readers correctly\",f\n1,2\n", []string{"here_is_my_very_long_quoted_column_that_will_get_only_parsed_if_we_pass_in_our_readers_correctly", "f"}, compressionNone, "test.csv"},
	}

	tdir := t.TempDir()

	for _, test := range tests {
		d, err := NewDatabase("", nil)
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(d.Config.WorkingDirectory)
		bf := new(bytes.Buffer)
		switch test.compression {
		case compressionNone:
			bf.Write([]byte(test.contents))
		case compressionGzip:
			gw := gzip.NewWriter(bf)
			gw.Write([]byte(test.contents))
			gw.Close()
		default:
			t.Fatalf("unsupported compression for writing: %+v", test.compression)
		}

		// first try from a reader
		ds, err := d.LoadDatasetFromReaderAuto("dataset", bytes.NewReader(bf.Bytes()))
		if err != nil {
			t.Fatal(err)
		}
		dsCols := make([]string, 0)
		for _, col := range ds.Schema {
			dsCols = append(dsCols, col.Name)
		}
		if !reflect.DeepEqual(dsCols, test.columns) {
			t.Errorf("expecting columns to be %+v, got %+v", test.columns, dsCols)
		}

		// but also from a file
		tfn := filepath.Join(tdir, test.filename)
		if err := os.WriteFile(tfn, bf.Bytes(), os.ModePerm); err != nil {
			t.Fatal(err)
		}
		ds, err = d.loadDatasetFromLocalFileAuto("dataset", tfn)
		if err != nil {
			t.Fatal(err)
		}
		dsCols = make([]string, 0)
		for _, col := range ds.Schema {
			dsCols = append(dsCols, col.Name)
		}
		if !reflect.DeepEqual(dsCols, test.columns) {
			t.Errorf("expecting columns to be %+v, got %+v", test.columns, dsCols)
		}
	}
}

func TestInabilityToInferTypes(t *testing.T) {
	db, err := NewDatabase("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()

	buf := strings.NewReader("foo,bar,baz\n")

	_, err = db.LoadDatasetFromReaderAuto("dataset", buf)
	if !errors.Is(err, errCannotInferTypes) {
		t.Fatalf("expecting to err with %v, got %v instead", errCannotInferTypes, err)
	}
}

func TestReadingFromStripes(t *testing.T) {
	db, err := NewDatabase("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()

	buf := strings.NewReader("foo,bar,baz\n1,true,1.23\n1444,,1e8")

	ds, err := db.LoadDatasetFromReaderAuto("dataset", buf)
	if err != nil {
		t.Fatal(err)
	}
	sr, err := NewStripeReader(db, ds, ds.Stripes[0])
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()
	col, err := sr.ReadColumn(0)
	if err != nil {
		t.Fatal(err)
	}
	if col.Len() != 2 {
		t.Errorf("expecting the length to be %+v, got %+v", 2, col.Len())
	}

	col, err = sr.ReadColumn(1)
	if err != nil {
		t.Fatal(err)
	}
	if col.Len() != 2 {
		t.Errorf("expecting the length to be %+v, got %+v", 2, col.Len())
	}

	col, err = sr.ReadColumn(2)
	if err != nil {
		t.Fatal(err)
	}
	if col.Len() != 2 {
		t.Errorf("expecting the length to be %+v, got %+v", 2, col.Len())
	}
}

// note that this measures throughput in terms of the original file size, not the size it takes on the disk
func BenchmarkReadingFromStripes(b *testing.B) {
	db, err := NewDatabase("", nil)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()

	header := "foo,bar,baz\n"
	row := "123456,true,1.234567891\n"
	for _, nrows := range []int{1, 100, 1000, 1000_000} {
		bName := strconv.Itoa(nrows)
		b.Run(bName, func(b *testing.B) {
			buf := new(bytes.Buffer)
			if _, err := buf.WriteString(header); err != nil {
				b.Fatal(err)
			}
			for j := 0; j < nrows; j++ {
				if _, err := buf.WriteString(row); err != nil {
					b.Fatal(err)
				}
			}

			b.SetBytes(int64(buf.Len()))

			ds, err := db.LoadDatasetFromReaderAuto("dataset", buf)
			if err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			for j := 0; j < b.N; j++ {
				crows := 0
				for _, stripe := range ds.Stripes {
					sr, err := NewStripeReader(db, ds, stripe)
					if err != nil {
						b.Fatal(err)
					}
					defer sr.Close()
					for cn := 0; cn < 3; cn++ {
						col, err := sr.ReadColumn(cn)
						if err != nil {
							b.Fatal(err)
						}
						if cn == 0 {
							crows += col.Len()
						}
					}
					sr.Close()

				}
				if crows != nrows {
					b.Errorf("expecting %v rows, got %v", nrows, crows)
				}
			}
		})
	}
}

func TestColumnSchemaMarshalingRoundtrips(t *testing.T) {
	cs := column.Schema{Name: "foo", Dtype: column.DtypeBool, Nullable: true}
	dt, err := json.Marshal(cs)
	if err != nil {
		t.Fatal(err)
	}
	var cs2 column.Schema
	if err := json.Unmarshal(dt, &cs2); err != nil {
		t.Fatal(err)
	}
}

func TestLoadingSampleData(t *testing.T) {
	db, err := NewDatabase("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()

	tmpdir := t.TempDir()
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

		if err := os.WriteFile(tfn, data, os.ModePerm); err != nil {
			t.Fatal(err)
		}
	}
	tmpfs := os.DirFS(tmpdir)
	if err := db.LoadSampleData(tmpfs); err != nil {
		t.Fatal(err)
	}

	if len(db.Datasets) != len(fns) {
		t.Errorf("expecting %+v datasets, got %+v", len(fns), len(db.Datasets))
	}

	ecols := []string{"foo", "bar", "baz"}
	for _, ds := range db.Datasets {
		cols := make([]string, 0)
		for _, col := range ds.Schema {
			cols = append(cols, col.Name)
		}
		if !reflect.DeepEqual(cols, ecols) {
			t.Errorf("expecting each dataset to have the header of %+v, got %+v", ecols, cols)
		}
	}
}

func TestLoadingSampleDataErrs(t *testing.T) {
	db, err := NewDatabase("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()

	tmpdir := t.TempDir()

	// now let's write some invalid data and expect it to fail for that reason
	if err := os.WriteFile(filepath.Join(tmpdir, "sample.csv"), []byte("foo\""), os.ModePerm); err != nil {
		t.Fatal(err)
	}

	tmpdirfs := os.DirFS(tmpdir)
	if err := db.LoadSampleData(tmpdirfs); !errors.Is(err, csv.ErrBareQuote) {
		t.Errorf("invalid data in a sample directory, expecting a CSV error to bubble up, got %+v instead", err)
	}
}

func TestBasicFileCaching(t *testing.T) {
	tmpdir := t.TempDir()
	for _, size := range []int{0, 1000, 1000_1000} {
		buf := new(bytes.Buffer)
		for j := 0; j < size; j++ {
			if _, err := buf.Write([]byte{byte(j % 256)}); err != nil {
				t.Fatal(err)
			}
		}
		rd := bytes.NewReader(buf.Bytes())
		path := filepath.Join(tmpdir, strconv.Itoa(size))
		if err := CacheIncomingFile(rd, path); err != nil {
			t.Error(err)
			continue
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(contents, buf.Bytes()) {
			t.Errorf("roundtrip failed for %+v bytes", size)
		}
	}
}

func TestCacheErrors(t *testing.T) {
	nopath := filepath.Join(t.TempDir(), "does_not_exist", "no_file.txt")

	data := strings.NewReader("ahoy")
	if err := CacheIncomingFile(data, nopath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("cannot cache into a non-existent directory, but got %+v", err)
	}
}

// if we flip any single bit in the file - apart from the checksums and version, we should get a checksum error
func TestChecksumValidation(t *testing.T) {
	db, err := NewDatabase("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()

	buf := strings.NewReader("foo,bar,baz\n1,true,1.23\n1444,,1e8")

	ds, err := db.LoadDatasetFromReaderAuto("dataset", buf)
	if err != nil {
		t.Fatal(err)
	}
	// this should work fine
	stripe := ds.Stripes[0]
	readStripes := func() error {
		sr, err := NewStripeReader(db, ds, stripe)
		if err != nil {
			return err
		}
		defer sr.Close()
		for colNum := 0; colNum < 3; colNum++ {
			_, err := sr.ReadColumn(colNum)
			if err != nil {
				return err
			}
		}
		return nil
	}
	if err := readStripes(); err != nil {
		t.Fatal(err)
	}
	path := db.stripePath(ds, stripe)
	stripeData, err := os.ReadFile(path)
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
			if err := os.WriteFile(path, mut, os.ModePerm); err != nil {
				t.Error(err)
				continue
			}
			if err := readStripes(); err != errIncorrectChecksum {
				t.Errorf("flipping bits should trigger %+v, got %+v instead", errIncorrectChecksum, err)
			}
		}
	}
}

func TestInvalidOffsets(t *testing.T) {
	db, err := NewDatabase("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()

	buf := strings.NewReader("foo,bar,baz\n1,true,1.23\n1444,,1e8")

	ds, err := db.LoadDatasetFromReaderAuto("dataset", buf)
	if err != nil {
		t.Fatal(err)
	}

	tests := [][]uint32{
		{1, 2, 3},  // not enough space for even a checksum
		{1, 0, 3},  // lower offset than the previous (sort of covered by the space criterion as well)
		{1, 5, 12}, // need space for a checksum and compression (5 bytes)
	}

	cols := []string{"foo", "bar", "baz"}
	for _, test := range tests {
		ds.Stripes[0].Offsets = test

		if _, _, err := db.ReadColumnsFromStripeByNames(ds, ds.Stripes[0], cols); err != errInvalidOffsetData {
			t.Errorf("expecting offsets %+v to trigger errInvalidOffsetData, but got %+v instead", test, err)
		}
	}
}

func TestHeaderValidation(t *testing.T) {
	tests := []struct {
		header      []string
		schemaNames []string
		err         error
	}{
		{[]string{"foo", "bar"}, []string{"foo", "bar"}, nil},
		{[]string{""}, []string{""}, nil},
		{[]string{"foo"}, []string{"bar", "bak"}, errSchemaMismatch},
		{[]string{"foo", "bar"}, []string{"bak"}, errSchemaMismatch},
		{[]string{"foo", "bar"}, []string{"foo", "bar "}, errSchemaMismatch},
	}
	for _, test := range tests {
		schema := make(column.TableSchema, 0, len(test.schemaNames))
		for _, el := range test.schemaNames {
			schema = append(schema, column.Schema{Name: el})
		}
		if err := validateHeaderAgainstSchema(test.header, schema); err != test.err {
			t.Fatalf("expected validation of header %+v and schema %+v to result in %+v, got %+v instead", test.header, test.schemaNames, test.err, err)
		}
	}
}

func TestLoadingFromMaps(t *testing.T) {
	db, err := NewDatabase("", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()
	tests := []struct {
		data   map[string][]string
		header []string
		length int // not checking this, remove?
		err    error
	}{
		{nil, nil, 0, errNoMapData},
		{map[string][]string{}, nil, 0, errNoMapData},
		{map[string][]string{
			"foo": {"1", "2", "3"},
		}, []string{"foo"}, 3, nil},
		{map[string][]string{
			"foo": {"1", "2", "3"},
			"bar": {"ahoy", "", "bak"},
		}, []string{"bar", "foo"}, 3, nil},
		{map[string][]string{
			"foo": {"1", "2", "3"},
			"bar": {"ahoy", "", "bak", "extra data"},
		}, nil, 0, errLengthMismatch},
	}

	for _, test := range tests {
		ds, err := db.LoadDatasetFromMap("dataset", test.data)
		if !errors.Is(err, test.err) {
			t.Errorf("expecting %+v to fail with %+v, got %+v instead", test.data, test.err, err)
			continue
		}
		if err != nil {
			continue
		}
		var columns []string
		for _, col := range ds.Schema {
			columns = append(columns, col.Name)
		}
		if !reflect.DeepEqual(columns, test.header) {
			t.Errorf("expecting %+v to result in %+v columns, got %+v instead", test.data, test.header, columns)
			continue
		}
	}

}

// func newRawLoader(r io.Reader, settings loadSettings) (*rawLoader, error) {
// func (ds *dataStripe) writeToWriter(w io.Writer) error {
// func (ds *dataStripe) writeToFile(rootDir, datasetID string) error { -- signature has changed, it's now writeStripeToFile
// func newStripeFromReader(rr RowReader, schema column.TableSchema, maxRows, maxBytes int) (*dataStripe, error) {
// func (db *Database) loadDatasetFromReader(r io.Reader, settings loadSettings) (*Dataset, error) {
// func (db *Database) loadDatasetFromLocalFile(path string, settings loadSettings) (*Dataset, error) {
// ReadColumnsFromStripeByNames
