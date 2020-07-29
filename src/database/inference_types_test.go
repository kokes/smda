package database

import (
	"encoding/csv"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/kokes/smda/src/column"
)

func TestDatasetTypeInference(t *testing.T) {
	db, err := NewDatabase(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Drop(); err != nil {
			panic(err)
		}
	}()

	datasets := []struct {
		raw string
		cs  TableSchema
	}{
		{"foo\n1\n2", TableSchema{{Name: "foo", Dtype: column.DtypeInt, Nullable: false}}},
		{"foo,bar\n1,2\n2,false", TableSchema{{Name: "foo", Dtype: column.DtypeInt, Nullable: false}, {Name: "bar", Dtype: column.DtypeString, Nullable: false}}},
		{"foo\ntrue\nFALSE", TableSchema{{Name: "foo", Dtype: column.DtypeBool, Nullable: false}}},
		{"foo,bar\na,b\nc,", TableSchema{{Name: "foo", Dtype: column.DtypeString, Nullable: false}, {Name: "bar", Dtype: column.DtypeString, Nullable: true}}}, // we do have nullable strings
		{"foo,bar\n1,\n2,3", TableSchema{{Name: "foo", Dtype: column.DtypeInt, Nullable: false}, {Name: "bar", Dtype: column.DtypeInt, Nullable: true}}},
		{"foo,bar\n1,\n2,", TableSchema{{Name: "foo", Dtype: column.DtypeInt, Nullable: false}, {Name: "bar", Dtype: column.DtypeNull, Nullable: true}}},
		// the following issues are linked to the fact that encoding/csv skips empty rows (???)
		// {"foo\n\n\n", TableSchema{{"foo", column.DtypeNull, true}}}, // this should work, but we keep returning invalid
		// {"foo\ntrue\n", TableSchema{{"foo", column.DtypeBool, true}}}, // this should be nullable, but we keep saying it is not
		// {"foo\nfoo\n\ntrue", TableSchema{{"foo", column.DtypeBool, true}}}, // this should be nullable, but we keep saying it is not
	}
	for _, dataset := range datasets {
		f, err := ioutil.TempFile("", "")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(f.Name())
		if err := CacheIncomingFile(strings.NewReader(dataset.raw), f.Name()); err != nil {
			t.Fatal(err)
		}
		cs, err := InferTypes(f.Name(), &loadSettings{})
		if err != nil {
			t.Error(err)
			continue
		}
		if !reflect.DeepEqual(cs, dataset.cs) {
			t.Errorf("expecting %v to be inferred as %v, got %v", dataset.raw, dataset.cs, cs)
		}
	}
}

func TestInferTypesNoFile(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "infer")
	if err != nil {
		t.Fatal(err)
	}
	filename := filepath.Join(tmpdir, "does_not_exist.csv")
	if _, err := InferTypes(filename, nil); !os.IsNotExist(err) {
		t.Errorf("expecting type inference on a non-existent file to throw a file not found error, got: %v", err)
	}
}

func TestInferTypesEmptyFile(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "infer")
	if err != nil {
		t.Fatal(err)
	}
	filename := filepath.Join(tmpdir, "filename.csv")
	f, err := os.Create(filename)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	if _, err := InferTypes(filename, &loadSettings{}); err != io.EOF {
		t.Errorf("expecting type inference on a non-existent file to throw a file not found error, got: %v", err)
	}
}

func TestInferTypesInvalidCSV(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "infer")
	if err != nil {
		t.Fatal(err)
	}
	filename := filepath.Join(tmpdir, "filename.csv")
	if err := ioutil.WriteFile(filename, []byte("\"ahoy"), os.ModePerm); err != nil {
		t.Fatal(err)
	}

	if _, err := InferTypes(filename, &loadSettings{}); !errors.Is(err, csv.ErrQuote) {
		t.Errorf("type inference on an invalid CSV should throw a native error, csv.ErrQuote in this case, but got: %v", err)
	}
}

func TestInferTypesNoloadSettings(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "infer")
	if err != nil {
		t.Fatal(err)
	}
	filename := filepath.Join(tmpdir, "filename.csv")
	f, err := os.Create(filename)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	if _, err := InferTypes(filename, nil); err != errInvalidloadSettings {
		t.Errorf("when inferring types from a CSV, we need to submit load settings - did not submit them, but didn't get errInvalidloadSettings, got: %v", err)
	}
}
