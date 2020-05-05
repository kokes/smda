package smda

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"testing"
)

func TestBasicStringColumn(t *testing.T) {
	tt := [][]string{
		{"foo", "bar", "baz"},
		{},
		{"", "", "", "foo", ""},
		{""},
	}
	for _, vals := range tt {
		nc := newColumnStrings(false)
		if err := nc.addValues(vals); err != nil {
			t.Error(err)
		}
		// TODO: this is the only test with roundtrips, because we don't have nth value implemented anywhere else
		// that's because we would have to have interface{} as the return value, and that's no good for individual values
		for j, val := range vals {
			got := nc.nthValue(j)
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
		{"1", "2", "3"},
		{"1", "2", "30923091239123"},
		{"-1", "2", "30923091239123"},
		{"0", "-0"},
		{},
		{strconv.Itoa(math.MaxInt64), strconv.Itoa(math.MinInt64)},
		{"1", "2", ""},
	}
	for _, vals := range tt {
		nc := newColumnInts(true)
		if err := nc.addValues(vals); err != nil {
			t.Error(err)
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
		{"1", "2", "3"},
		{"+1", "-2", "+0"},
		{".1", ".2", ".3"},
		{"0", "-0", "+0"},
		{strconv.FormatFloat(math.MaxInt64, 'E', -1, 64), strconv.FormatFloat(math.MinInt64, 'E', -1, 64)},
		{strconv.FormatFloat(math.MaxFloat64, 'E', -1, 64), strconv.FormatFloat(math.SmallestNonzeroFloat64, 'E', -1, 64)},
		{strconv.FormatFloat(math.MaxFloat32, 'E', -1, 32), strconv.FormatFloat(math.SmallestNonzeroFloat32, 'E', -1, 32)},
		{"nan", "NAN"},
		{},
		{"", "", ""}, // -> nulls
		{"1", "", "1.2"},
	}
	for _, vals := range tt {
		nc := newColumnFloats(true)
		if err := nc.addValues(vals); err != nil {
			t.Error(err)
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
		{"true", "false"},
		{"true", "FALSE"},
		{"T", "F"},
		{},
		{"T", "F", ""},
	}
	for _, vals := range tt {
		nc := newColumnBools(true)
		if err := nc.addValues(vals); err != nil {
			t.Error(err)
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
		if err := col.addValues(test.vals); err != nil {
			t.Error(err)
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
		if err := test.rc.addValues(test.values); err != nil {
			t.Error(err)
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
		dtype  dtype
		values []string
		op     operator
		val    string
		count  int
	}{
		{dtypeBool, []string{"true", "false", "true"}, opEqual, "true", 2},
		{dtypeBool, []string{"false", "true", "false"}, opEqual, "false", 2},
		{dtypeBool, []string{"false", "false", "false"}, opEqual, "true", 0},
		{dtypeBool, []string{"false", "false", "false"}, opNotEqual, "false", 0},
		{dtypeBool, []string{"false", "true", "false"}, opNotEqual, "false", 1},

		{dtypeInt, []string{"1", "2", "3"}, opEqual, "0", 0},
		{dtypeInt, []string{"1", "2", "3"}, opEqual, "3", 1},
		{dtypeInt, []string{"1", "2", "3"}, opEqual, "10000", 0},
		{dtypeInt, []string{"1", "2", "3"}, opNotEqual, "1", 2},
		{dtypeInt, []string{"1", "2", "3"}, opNotEqual, "4", 3},
		{dtypeInt, []string{"1", "1", "1"}, opNotEqual, "1", 0},
		{dtypeInt, []string{"1", "1", "1"}, opGt, "0", 3},
		{dtypeInt, []string{"1", "2", "3"}, opGt, "2", 1},
		{dtypeInt, []string{"1", "2", "3"}, opGte, "2", 2},
		{dtypeInt, []string{"1", "2", "3"}, opLt, "6", 3},
		{dtypeInt, []string{"1", "2", "3"}, opLte, "2", 2},

		{dtypeFloat, []string{"1.23", "+0", "-0", "1e3"}, opEqual, "1.2300", 1},
		{dtypeFloat, []string{"1.23", "+0", "-0", "1e3"}, opEqual, "1.230000001", 0},
		{dtypeFloat, []string{"1.23", "+0", "-0", "1e3"}, opEqual, "+0", 2},
		{dtypeFloat, []string{"1.23", "+0", "-0", "1e3"}, opEqual, "1000", 1},
		{dtypeFloat, []string{"1.23", "+0", "-0", "1e3"}, opNotEqual, "0", 2},
		{dtypeFloat, []string{"1.23", "+0", "-0", "1e3"}, opNotEqual, "1000", 3},
		{dtypeFloat, []string{"1.23", "+0", "-0", "1e3"}, opNotEqual, "1234", 4},
		{dtypeFloat, []string{"1", "1", "1"}, opGt, "0", 3},
		{dtypeFloat, []string{"1", "2", "3"}, opGt, "2", 1},
		{dtypeFloat, []string{"1", "2", "3"}, opGte, "2", 2},
		{dtypeFloat, []string{"1", "2", "3"}, opLt, "6", 3},
		{dtypeFloat, []string{"1", "2", "3"}, opLte, "2", 2},

		{dtypeString, []string{"foo", "bar", "baz", "foo"}, opEqual, "baz", 1},
		{dtypeString, []string{"foo", "bar", "baz", "foo"}, opEqual, "foo", 2},
		{dtypeString, []string{"foo", "bar", "baz", "foo"}, opEqual, "FOO", 0},
		{dtypeString, []string{"foo", "bar", "baz", "foo"}, opNotEqual, "foo", 2},
		{dtypeString, []string{"foo", "bar", "baz", "foo"}, opNotEqual, "FOO", 4},

		// we don't need to test null columns, because we might just delete all the opEqual code, it probably
		// isn't useful for anyone
	}
	for _, test := range tests {
		for _, nullable := range []bool{true, false} {
			rc := newTypedColumnFromSchema(columnSchema{Dtype: test.dtype, Nullable: nullable})
			// for nullable columns, sprinkle in some null values in the mix and make sure the filter
			// works the same way
			for j, val := range test.values {
				// no support for nullable strings (or rather their addition), so we're exluding them for now
				if nullable && j%2 == 0 && test.dtype != dtypeString {
					if err := rc.addValue(""); err != nil {
						t.Fatal(err)
					}
				}
				if err := rc.addValue(val); err != nil {
					t.Fatal(err)
				}
			}

			filtered := rc.Filter(test.op, test.val)
			count := 0
			if filtered != nil {
				count = filtered.Count()
			}
			if count != test.count {
				t.Errorf("expected that filtering %v using %v in %v would result in %v rows, got %v", test.val, test.op, test.values, test.count, count)
			}
		}
	}
}

func TestBasicPruning(t *testing.T) {
	tests := []struct {
		dtype    dtype
		nullable bool
		values   []string
		bools    []bool
		expected []string
	}{
		{dtypeBool, false, []string{"true", "false", "true"}, []bool{true, true, true}, []string{"true", "false", "true"}},
		{dtypeBool, false, []string{"true", "false", "true"}, []bool{false, false, false}, nil},
		{dtypeBool, false, []string{"true", "false", "true"}, []bool{false, true, false}, []string{"false"}},

		{dtypeInt, false, []string{"1", "2", "3"}, []bool{false, true, false}, []string{"2"}},
		{dtypeFloat, false, []string{"1.23", "+0", "1e3"}, []bool{false, true, false}, []string{"0"}},
		{dtypeString, false, []string{"foo", "bar", "foo"}, []bool{false, true, false}, []string{"bar"}},

		// nullable columns
		{dtypeInt, true, []string{"1", "", ""}, []bool{false, true, false}, []string{""}},
		{dtypeInt, true, []string{"1", "", ""}, []bool{false, false, false}, nil},
		{dtypeInt, true, []string{"1", "", ""}, []bool{true, true, true}, []string{"1", "", ""}},

		{dtypeBool, true, []string{"true", "", "true"}, []bool{true, true, false}, []string{"t", ""}},
		{dtypeFloat, true, []string{"1.23", "+0", ""}, []bool{false, true, false}, []string{"0"}},
		{dtypeString, true, []string{"foo", "", ""}, []bool{true, true, true}, []string{"foo", "", ""}},

		// not pruning anything by leveraging nil pointers
		{dtypeInt, true, []string{"1", "", ""}, nil, nil},
		{dtypeBool, true, []string{"true", "", "true"}, nil, nil},
		{dtypeFloat, true, []string{"1.23", "+0", ""}, nil, nil},
		{dtypeString, true, []string{"foo", "", ""}, nil, nil},
	}
	for _, test := range tests {
		testSchema := columnSchema{Dtype: test.dtype, Nullable: test.nullable}
		rc := newTypedColumnFromSchema(testSchema)
		if err := rc.addValues(test.values); err != nil {
			t.Error(err)
			continue
		}

		var bm *Bitmap
		if test.bools != nil {
			bm = NewBitmapFromBools(test.bools)
		}
		pruned := rc.Prune(bm)
		expected := newTypedColumnFromSchema(testSchema)
		if err := expected.addValues(test.expected); err != nil {
			t.Error(err)
			continue
		}
		if !reflect.DeepEqual(pruned, expected) {
			t.Errorf("expected that pruning %v using %v would result in %v", test.values, test.bools, test.expected)
		}
	}
}

func TestFilterAndPrune(t *testing.T) {
	// TODO
}

func TestAppending(t *testing.T) {
	tests := []struct {
		dtype    dtype
		nullable bool
		a        []string
		b        []string
		res      []string
	}{
		{dtypeString, false, []string{"1", "2", "3"}, []string{"4", "5", "6"}, []string{"1", "2", "3", "4", "5", "6"}},
		{dtypeInt, false, []string{"1", "2", "3"}, []string{"4", "5", "6"}, []string{"1", "2", "3", "4", "5", "6"}},
		{dtypeFloat, false, []string{"1", "2", "3"}, []string{"4", "5", "6"}, []string{"1", "2", "3", "4", "5", "6"}},
		{dtypeBool, false, []string{"T", "F", "T"}, []string{"F", "F", "T"}, []string{"T", "F", "T", "F", "F", "T"}},

		// nullable (makes no sense for strings, we don't support them?)
		// {dtypeString, true, []string{"1", "2", "3"}, []string{"4", "5", "6"}, []string{"1", "2", "3", "4", "5", "6"}},
		// {dtypeString, true, []string{"1", "", "3"}, []string{"4", "5", ""}, []string{"1", "", "3", "4", "5", ""}},
		{dtypeInt, true, []string{"1", "", "3"}, []string{"4", "5", ""}, []string{"1", "", "3", "4", "5", ""}},
		// TODO: I think this hits https://github.com/golang/go/issues/12025 - failure for reflect.DeepEqual to compare nested NaNs
		// {dtypeFloat, true, []string{"1", "", "3"}, []string{"4", "5", ""}, []string{"1", "", "3", "4", "5", ""}},
		{dtypeBool, true, []string{"", "", ""}, []string{"F", "F", ""}, []string{"", "", "", "F", "F", ""}},
	}
	for _, test := range tests {
		rc := newTypedColumnFromSchema(columnSchema{Dtype: test.dtype, Nullable: test.nullable})
		nrc := newTypedColumnFromSchema(columnSchema{Dtype: test.dtype, Nullable: test.nullable})
		rrc := newTypedColumnFromSchema(columnSchema{Dtype: test.dtype, Nullable: test.nullable})

		if err := rc.addValues(test.a); err != nil {
			t.Error(err)
		}
		if err := nrc.addValues(test.b); err != nil {
			t.Error(err)
		}
		if err := rrc.addValues(test.res); err != nil {
			t.Error(err)
		}
		if err := rc.Append(nrc); err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(rc, rrc) {
			fmt.Println(rc.(*columnFloats).nullability)
			fmt.Println(rrc.(*columnFloats).nullability)
			fmt.Println(rc, rrc)
			t.Errorf("expected that %v plus %v results in %v", test.a, test.b, test.res)
		}

	}
}

func TestHashing(t *testing.T) {
	tests := []struct {
		dtype dtype
		data  []string
	}{
		{dtypeString, []string{"foo", "bar", "baz"}},
	}

	for _, test := range tests {
		rc := newTypedColumnFromSchema(columnSchema{Dtype: test.dtype})
		if err := rc.addValues(test.data); err != nil {
			t.Fatal(err)
		}
		hashes1 := make([]uint64, len(test.data))
		hashes2 := make([]uint64, len(test.data))
		rc.Hash(hashes1)
		rc.Hash(hashes2)

		if !reflect.DeepEqual(hashes1, hashes2) {
			t.Errorf("hashing twice did not result in the same slice: %v vs. %v", hashes1, hashes2)
		}
	}
}

// TODO: we absolutely need to make sure the column spans more stripes,
// so that we can test that we don't mess with the seeds or anything (e.g. using
// plain maphash would pass tests, but it would be very incorrect)
func BenchmarkHashingInts(b *testing.B) {
	n := 10000
	col := newColumnInts(false)
	for j := 0; j < n; j++ {
		col.addValue(strconv.Itoa(j))
	}
	b.ResetTimer()

	hashes := make([]uint64, col.Len())
	for j := 0; j < b.N; j++ {
		col.Hash(hashes)
	}
	b.SetBytes(int64(8 * n))
}

// func newTypedColumnFromSchema(schema columnSchema) typedColumn {

// tests for columnNulls
