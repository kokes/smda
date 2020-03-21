package smda

import (
	"bytes"
	"encoding/json"
	"math"
	"reflect"
	"strconv"
	"testing"
)

func TestBasicStringColumn(t *testing.T) {
	tt := [][]string{
		[]string{"foo", "bar", "baz"},
		[]string{},
		[]string{"", "", "", "foo", ""},
		[]string{""},
	}
	for _, vals := range tt {
		nc := newColumnStrings(false)
		for _, val := range vals {
			if err := nc.addValue(val); err != nil {
				t.Error(err)
				return
			}
		}
		// TODO: this is the only test with roundtrips, because we don't have nth value implemented anywhere else
		// that's because we would have to have interface{} as the return value, and that's no good for individual values
		for j, val := range vals {
			got := nc.nthValue(uint32(j))
			if got != val {
				t.Errorf("expecting %v, got %v", val, got)
				return
			}
		}

		buf := new(bytes.Buffer)
		n, err := nc.serializeInto(buf)
		if err != nil {
			t.Error(err)
			return
		}
		if int(n) != len(buf.Bytes()) {
			t.Errorf("wrote %v bytes, but reported %v", len(buf.Bytes()), n)
		}
	}
}

func TestBasicIntColumn(t *testing.T) {
	tt := [][]string{
		[]string{"1", "2", "3"},
		[]string{"1", "2", "30923091239123"},
		[]string{"-1", "2", "30923091239123"},
		[]string{"0", "-0"},
		[]string{},
		[]string{strconv.Itoa(math.MaxInt64), strconv.Itoa(math.MinInt64)},
		[]string{"1", "2", ""},
	}
	for _, vals := range tt {
		nc := newColumnInts(true)
		for _, val := range vals {
			if err := nc.addValue(val); err != nil {
				t.Error(err)
				return
			}
		}

		buf := new(bytes.Buffer)
		n, err := nc.serializeInto(buf)
		if err != nil {
			t.Error(err)
			return
		}
		if int(n) != len(buf.Bytes()) {
			t.Errorf("wrote %v bytes, but reported %v", len(buf.Bytes()), n)
		}

	}
}

// what about infites?
func TestBasicFloatColumn(t *testing.T) {
	tt := [][]string{
		[]string{"1", "2", "3"},
		[]string{"+1", "-2", "+0"},
		[]string{".1", ".2", ".3"},
		[]string{"0", "-0", "+0"},
		[]string{strconv.FormatFloat(math.MaxInt64, 'E', -1, 64), strconv.FormatFloat(math.MinInt64, 'E', -1, 64)},
		[]string{strconv.FormatFloat(math.MaxFloat64, 'E', -1, 64), strconv.FormatFloat(math.SmallestNonzeroFloat64, 'E', -1, 64)},
		[]string{strconv.FormatFloat(math.MaxFloat32, 'E', -1, 32), strconv.FormatFloat(math.SmallestNonzeroFloat32, 'E', -1, 32)},
		[]string{"nan", "NAN"},
		[]string{},
		[]string{"", "", ""}, // -> nulls
		[]string{"1", "", "1.2"},
	}
	for _, vals := range tt {
		nc := newColumnFloats(true)
		for _, val := range vals {
			if err := nc.addValue(val); err != nil {
				t.Error(err)
				return
			}
		}

		buf := new(bytes.Buffer)
		n, err := nc.serializeInto(buf)
		if err != nil {
			t.Error(err)
			return
		}
		if int(n) != len(buf.Bytes()) {
			t.Errorf("wrote %v bytes, but reported %v", len(buf.Bytes()), n)
		}

	}
}

func TestBasicBoolColumn(t *testing.T) {
	tt := [][]string{
		[]string{"true", "false"},
		[]string{"true", "FALSE"},
		[]string{"T", "F"},
		[]string{"True", "False"}, // not sure I like this
		[]string{},
		[]string{"T", "F", ""},
	}
	for _, vals := range tt {
		nc := newColumnBools(true)
		for _, val := range vals {
			if err := nc.addValue(val); err != nil {
				t.Error(err)
				return
			}
		}

		buf := new(bytes.Buffer)
		n, err := nc.serializeInto(buf)
		if err != nil {
			t.Error(err)
			return
		}
		if int(n) != len(buf.Bytes()) {
			t.Errorf("wrote %v bytes, but reported %v", len(buf.Bytes()), n)
		}

	}
}

func TestInvalidInts(t *testing.T) {
	tt := []string{"1.", ".1", "1e3"}

	for _, testCase := range tt {
		nc := newColumnInts(false)
		if err := nc.addValue(testCase); err == nil {
			t.Errorf("did not expect \"%v\" to not be a valid int", testCase)
		}
	}
}

func TestInvalidFloats(t *testing.T) {
	tt := []string{"1e123003030303", ".e", "123f"}

	for _, testCase := range tt {
		nc := newColumnFloats(false)
		if err := nc.addValue(testCase); err == nil {
			t.Errorf("did not expect \"%v\" to not be a valid float", testCase)
		}
	}
}

func TestInvalidBools(t *testing.T) {
	tt := []string{"Y", "N", "YES", "NO"} // add True/False once we stop supporting it

	for _, testCase := range tt {
		nc := newColumnBools(false)
		if err := nc.addValue(testCase); err == nil {
			t.Errorf("did not expect \"%v\" to not be a valid bool", testCase)
		}
	}
}

// TODO: merge all the tests above into this one
func TestSerialisationRoundtrip(t *testing.T) {
	tests := []struct {
		schema columnSchema
		vals   []string
	}{
		{columnSchema{"", dtypeString, true}, []string{"foo", "bar", "baz"}},
		{columnSchema{"", dtypeString, false}, []string{"foo", "bar", "baz"}},
		{columnSchema{"", dtypeString, true}, []string{}},
		{columnSchema{"", dtypeString, true}, []string{""}},
		// add other types (from earlier tests)
	}
	for _, test := range tests {
		col := newTypedColumnFromSchema(test.schema)
		for _, val := range test.vals {
			col.addValue(val)
		}
		bf := new(bytes.Buffer)
		n, err := col.serializeInto(bf)
		if err != nil {
			t.Fatal(err)
		}
		if n != len(bf.Bytes()) {
			t.Fatalf("expected to write %v bytes, got %v instead", n, len(bf.Bytes()))
		}
		col2, err := deserializeColumn(bf, test.schema.Dtype)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(col, col2) {
			t.Fatalf("expecting %v, got %v", col, col2)
		}
	}
}

func TestJSONMarshaling(t *testing.T) {
	tests := []struct {
		rc       typedColumn // use newTypedColumnFromSchema instead
		values   []string
		expected string
	}{
		{newColumnBools(true), []string{}, "[]"},
		{newColumnBools(false), []string{}, "[]"},
		{newColumnBools(true), []string{"true", "false"}, "[true,false]"},
		{newColumnBools(false), []string{"true", "false"}, "[true,false]"},
		{newColumnBools(true), []string{"", "true", "", ""}, "[null,true,null,null]"},
		{newColumnInts(true), []string{}, "[]"},
		{newColumnInts(false), []string{}, "[]"},
		{newColumnInts(true), []string{"123", "456"}, "[123,456]"},
		{newColumnInts(false), []string{"123", "456"}, "[123,456]"},
		{newColumnInts(true), []string{"123", "", "", "456"}, "[123,null,null,456]"},
		{newColumnFloats(false), []string{"123", "456"}, "[123,456]"},
		{newColumnFloats(false), []string{"123.456", "456.789"}, "[123.456,456.789]"},
		{newColumnFloats(true), []string{"123", "", "456"}, "[123,null,456]"},
		{newColumnFloats(true), []string{"123", "", "nan"}, "[123,null,null]"},
		// {newColumnFloats(true), []string{"123", "+infty", "-infty"}, "[123,+infty,-infty]"}, // no infty support yet
		{newColumnStrings(true), []string{}, "[]"},
		{newColumnStrings(false), []string{}, "[]"},
		{newColumnStrings(true), []string{"foo", "bar"}, "[\"foo\",\"bar\"]"},
		{newColumnStrings(false), []string{"foo", "bar"}, "[\"foo\",\"bar\"]"},
		{newColumnNulls(), []string{""}, "[null]"},
		{newColumnNulls(), []string{"", "", ""}, "[null,null,null]"},

		// we don't really have nullable strings at this point
		// {newColumnStrings(true), []string{"", "bar", ""}, "[null,\"bar\",null]"},
	}
	for _, test := range tests {
		for _, val := range test.values {
			if err := test.rc.addValue(val); err != nil {
				t.Fatal(err)
			}
		}
		w := new(bytes.Buffer)
		if err := json.NewEncoder(w).Encode(test.rc); err != nil {
			t.Fatal(err)
		}
		got := bytes.TrimSpace(w.Bytes())
		if !bytes.Equal([]byte(test.expected), got) {
			t.Errorf("expecting %v, got %v", test.expected, string(got))
		}
	}
}

func TestBasicFilters(t *testing.T) {
	tests := []struct {
		rc     typedColumn // we don't need to initiate rcs here, we can supply dtype and nullability and use newTypedColumnFromSchema
		values []string
		op     operator
		val    string
		count  int
	}{
		{newColumnBools(false), []string{"true", "false", "true"}, opEqual, "true", 2},
		{newColumnBools(false), []string{"false", "true", "false"}, opEqual, "false", 2},
		{newColumnBools(false), []string{"false", "false", "false"}, opEqual, "true", 0},
		{newColumnBools(false), []string{"false", "false", "false"}, opNotEqual, "false", 0},
		{newColumnBools(false), []string{"false", "true", "false"}, opNotEqual, "false", 1},

		{newColumnInts(false), []string{"1", "2", "3"}, opEqual, "0", 0},
		{newColumnInts(false), []string{"1", "2", "3"}, opEqual, "3", 1},
		{newColumnInts(false), []string{"1", "2", "3"}, opEqual, "10000", 0},
		{newColumnInts(false), []string{"1", "2", "3"}, opNotEqual, "1", 2},
		{newColumnInts(false), []string{"1", "2", "3"}, opNotEqual, "4", 3},
		{newColumnInts(false), []string{"1", "1", "1"}, opNotEqual, "1", 0},

		{newColumnFloats(false), []string{"1.23", "+0", "-0", "1e3"}, opEqual, "1.2300", 1},
		{newColumnFloats(false), []string{"1.23", "+0", "-0", "1e3"}, opEqual, "1.230000001", 0},
		{newColumnFloats(false), []string{"1.23", "+0", "-0", "1e3"}, opEqual, "+0", 2},
		{newColumnFloats(false), []string{"1.23", "+0", "-0", "1e3"}, opEqual, "1000", 1},
		{newColumnFloats(false), []string{"1.23", "+0", "-0", "1e3"}, opNotEqual, "0", 2},
		{newColumnFloats(false), []string{"1.23", "+0", "-0", "1e3"}, opNotEqual, "1000", 3},
		{newColumnFloats(false), []string{"1.23", "+0", "-0", "1e3"}, opNotEqual, "1234", 4},

		{newColumnStrings(false), []string{"foo", "bar", "baz", "foo"}, opEqual, "baz", 1},
		{newColumnStrings(false), []string{"foo", "bar", "baz", "foo"}, opEqual, "foo", 2},
		{newColumnStrings(false), []string{"foo", "bar", "baz", "foo"}, opEqual, "FOO", 0},
		{newColumnStrings(false), []string{"foo", "bar", "baz", "foo"}, opNotEqual, "foo", 2},
		{newColumnStrings(false), []string{"foo", "bar", "baz", "foo"}, opNotEqual, "FOO", 4},

		// we don't need to test null columns, because we might just delete all the opEqual code, it probably
		// isn't useful for anyone
	}
	for _, test := range tests {
		for _, val := range test.values {
			if err := test.rc.addValue(val); err != nil {
				t.Fatal(err)
			}
		}

		filtered := test.rc.Filter(test.op, test.val)
		count := 0
		if filtered != nil {
			count = filtered.Count()
		}
		if count != test.count {
			t.Errorf("expected that filtering %v using %v in %v would result in %v rows, got %v", test.val, test.op, test.values, test.count, count)
		}
	}
}

func TestBasicPruning(t *testing.T) {
	tests := []struct {
		rc     typedColumn
		values []string
		bm     *Bitmap
		count  int
	}{
		{newColumnBools(false), []string{"true", "false", "true"}, NewBitmapFromBools([]bool{true, true, true}), 3},
		{newColumnBools(false), []string{"true", "false", "true"}, NewBitmapFromBools([]bool{false, false, false}), 0},
		{newColumnBools(false), []string{"true", "false", "true"}, NewBitmapFromBools([]bool{false, true, false}), 1},

		{newColumnInts(false), []string{"1", "2", "3"}, NewBitmapFromBools([]bool{false, true, false}), 1},
		{newColumnFloats(false), []string{"1.23", "+0", "1e3"}, NewBitmapFromBools([]bool{false, true, false}), 1},
		{newColumnStrings(false), []string{"foo", "bar", "foo"}, NewBitmapFromBools([]bool{false, true, false}), 1},

		// nullable columns
		{newColumnInts(true), []string{"1", "", ""}, NewBitmapFromBools([]bool{false, true, false}), 1},
		{newColumnInts(true), []string{"1", "", ""}, NewBitmapFromBools([]bool{false, false, false}), 0},
		{newColumnInts(true), []string{"1", "", ""}, NewBitmapFromBools([]bool{true, true, true}), 3},

		{newColumnBools(true), []string{"true", "", "true"}, NewBitmapFromBools([]bool{true, true, false}), 2},
		{newColumnFloats(true), []string{"1.23", "+0", ""}, NewBitmapFromBools([]bool{false, true, false}), 1},
		{newColumnStrings(true), []string{"foo", "", ""}, NewBitmapFromBools([]bool{true, true, true}), 3},

		// not pruning anything by leveraging nil pointers
		{newColumnInts(true), []string{"1", "", ""}, nil, 0},
		{newColumnBools(true), []string{"true", "", "true"}, nil, 0},
		{newColumnFloats(true), []string{"1.23", "+0", ""}, nil, 0},
		{newColumnStrings(true), []string{"foo", "", ""}, nil, 0},
	}
	for _, test := range tests {
		for _, val := range test.values {
			if err := test.rc.addValue(val); err != nil {
				t.Fatal(err)
			}
		}

		pruned := test.rc.Prune(test.bm)
		count := int(pruned.Len())
		if count != test.count {
			t.Errorf("expected that pruning %v would result in %v rows, got %v", test.values, test.count, count)
		}
	}
}

func TestFilterAndPrune(t *testing.T) {
	// TODO
}

// func newTypedColumnFromSchema(schema columnSchema) typedColumn {

// tests for columnNulls
