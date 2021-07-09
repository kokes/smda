package database

import (
	"encoding/csv"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/kokes/smda/src/column"
)

func TestColumnCleanup(t *testing.T) {
	tests := []struct {
		// both are pipe delimited strings to avoid clunky slice syntax
		input    string
		expected string
	}{
		{"", "column_01"},
		{"foo|bar|baz", "foo|bar|baz"},
		{"foo | bar | baz", "foo|bar|baz"},
		{"foo |	bar	|	baz", "foo|bar|baz"},
		{"foo\n|	bar	|	baz", "foo|bar|baz"},
		{"||", "column_01|column_02|column_03"},
		{"foo|bar|foo", "foo|bar|foo_01"},
		{"Foo|bar", "foo|bar"},
		{"FOO|bar", "foo|bar"},
		{"ščě|bar", "column_01|bar"},
		{"|bar", "column_01|bar"},
		{"foo 23|bar", "foo_23|bar"},
		{"foo  - 23|bar", "foo_23|bar"},
		{"foo  ! 23|bar", "foo_23|bar"},
		{"a|b", "a|b"},
		{"a___b|b", "a_b|b"},
		{"a____b|b", "a_b|b"},
		{"a____b|a_b", "a_b|a_b_01"},
		{"a____b|a___b", "a_b|a_b_01"},
		// camel casing
		{"fooID", "foo_id"},
		{"fooId", "foo_id"},
		{"fooIdBarBaz", "foo_id_bar_baz"},
		// cannot parse identifiers that start with a digit (because we optimistically think of them as numbers)
		{"1foo", "column1foo"},
		{"č1foo", "column1foo"},
		{"_1foo", "column1foo"},
		{"123", "column123"},
	}

	for _, test := range tests {
		clean := cleanupColumns(strings.Split(test.input, "|"))
		expected := strings.Split(test.expected, "|")
		if !reflect.DeepEqual(clean, expected) {
			t.Errorf("expected columns %v to be cleaned up into %v, got %v instead", test.input, expected, clean)
		}
	}
}

func TestDatasetTypeInference(t *testing.T) {
	db, err := NewDatabase("", nil)
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
		cs  column.TableSchema
	}{
		{"foo\n1\n2", column.TableSchema{{Name: "foo", Dtype: column.DtypeInt, Nullable: false}}},
		{"foo,bar\n1,2\n2,false", column.TableSchema{{Name: "foo", Dtype: column.DtypeInt, Nullable: false}, {Name: "bar", Dtype: column.DtypeString, Nullable: false}}},
		{"foo\ntrue\nFALSE", column.TableSchema{{Name: "foo", Dtype: column.DtypeBool, Nullable: false}}},
		{"foo,bar\na,b\nc,", column.TableSchema{{Name: "foo", Dtype: column.DtypeString, Nullable: false}, {Name: "bar", Dtype: column.DtypeString, Nullable: true}}}, // we do have nullable strings
		{"foo,bar\n1,\n2,3", column.TableSchema{{Name: "foo", Dtype: column.DtypeInt, Nullable: false}, {Name: "bar", Dtype: column.DtypeInt, Nullable: true}}},
		{"foo,bar\n1,\n2,", column.TableSchema{{Name: "foo", Dtype: column.DtypeInt, Nullable: false}, {Name: "bar", Dtype: column.DtypeNull, Nullable: true}}},
		// the following issues are linked to the fact that encoding/csv skips empty rows (???)
		// {"foo\n\n\n", column.TableSchema{{"foo", column.DtypeNull, true}}}, // this should work, but we keep returning invalid
		// {"foo\ntrue\n", column.TableSchema{{"foo", column.DtypeBool, true}}}, // this should be nullable, but we keep saying it is not
		// {"foo\nfoo\n\ntrue", column.TableSchema{{"foo", column.DtypeBool, true}}}, // this should be nullable, but we keep saying it is not
	}
	for _, dataset := range datasets {
		f, err := os.CreateTemp("", "")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(f.Name())
		if err := CacheIncomingFile(strings.NewReader(dataset.raw), f.Name()); err != nil {
			t.Fatal(err)
		}
		cs, err := inferTypes(f.Name(), &loadSettings{})
		if err != nil {
			t.Error(err)
			continue
		}
		if !reflect.DeepEqual(cs, dataset.cs) {
			t.Errorf("expecting %+v to be inferred as %+v, got %+v", dataset.raw, dataset.cs, cs)
		}
	}
}

func TestInferTypesNoFile(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "does_not_exist.csv")
	if _, err := inferTypes(filename, nil); !os.IsNotExist(err) {
		t.Errorf("expecting type inference on a non-existent file to throw a file not found error, got: %+v", err)
	}
}

func TestInferTypesEmptyFile(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "filename.csv")
	f, err := os.Create(filename)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	if _, err := inferTypes(filename, &loadSettings{}); err != io.EOF {
		t.Errorf("expecting type inference on a non-existent file to throw a file not found error, got: %+v", err)
	}
}

func TestInferTypesInvalidCSV(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "filename.csv")
	if err := os.WriteFile(filename, []byte("\"ahoy"), os.ModePerm); err != nil {
		t.Fatal(err)
	}

	if _, err := inferTypes(filename, &loadSettings{}); !errors.Is(err, csv.ErrQuote) {
		t.Errorf("type inference on an invalid CSV should throw a native error, csv.ErrQuote in this case, but got: %+v", err)
	}
}

func TestInferTypesOnlyHeader(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "filename.csv")
	if err := os.WriteFile(filename, []byte("foo,bar\n"), os.ModePerm); err != nil {
		t.Fatal(err)
	}

	if _, err := inferTypes(filename, &loadSettings{}); !errors.Is(err, errCannotInferTypes) {
		t.Errorf("type inference on a header-only file should fail with %v, got %v instead", errCannotInferTypes, err)
	}
}

func TestInferTypesNoloadSettings(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "filename.csv")
	f, err := os.Create(filename)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	if _, err := inferTypes(filename, nil); err != errInvalidloadSettings {
		t.Errorf("when inferring types from a CSV, we need to submit load settings - did not submit them, but didn't get errInvalidloadSettings, got: %+v", err)
	}
}
