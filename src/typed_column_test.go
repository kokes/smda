package smda

import (
	"bytes"
	"encoding/json"
	"math"
	"reflect"
	"strconv"
	"testing"
)

// func TestBitmapSetsGets(t *testing.T) {
// 	vals := []bool{true, false, false, false, true, true, false}
// 	b := NewBitmap(0)
// 	for j, v := range vals {
// 		b.set(j, v)
// 	}
// 	fmt.Println(b)
// 	for j, v := range vals {
// 		if b.get(j) != v {
// 			t.Fatalf("expected %v, got %v", v, b.get(j))
// 		}
// 	}
// }

func TestBitmapRoundtrip(t *testing.T) {
	bitmaps := []*Bitmap{
		NewBitmapFromBools([]bool{true, false, true, false}),
		NewBitmap(0),
		NewBitmap(1),
		NewBitmap(9),
		NewBitmap(64),
		NewBitmap(1000_000),
	}
	for _, b := range bitmaps {
		bf := new(bytes.Buffer)
		_, err := b.serialize(bf)
		if err != nil {
			t.Error(err)
			return
		}
		br := bytes.NewReader(bf.Bytes())

		b2, err := deserialiseBitmapFromReader(br)
		if err != nil {
			t.Error(err)
			return
		}
		if !reflect.DeepEqual(b, b2) {
			t.Errorf("expecting %v, got %v", b, b2)
			return
		}
	}
}

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
			got := nc.nthValue(int64(j))
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
		nc := newColumnFloats()
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
		nc := newColumnFloats()
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

func TestSerialisationRoundtripStrings(t *testing.T) {
	tests := [][]string{
		[]string{"foo", "bar", "baz"},
		[]string{},
		[]string{"single value here"},
		// we currently don't have nullable string columns - there's no way to define what a null string is
		// if it's empty, it will just be an empty string
		// []string{"how", "about", "", "nulls?"},
	}
	for _, vals := range tests {
		col := newColumnStrings(true)
		for _, val := range vals {
			col.addValue(val)
		}
		bf := new(bytes.Buffer)
		if _, err := col.serializeInto(bf); err != nil {
			t.Fatal(err)
		}
		col2, err := deserializeColumnStrings(bytes.NewReader(bf.Bytes()))
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
		rc       typedColumn
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
		// TODO: floats
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

// func NewBitmap(n int) *bitmap {
// func NewBitmapFromBools(data []bool) *bitmap {
// func (b *bitmap) set(n int, val bool) {
// func (b *bitmap) get(n int) bool {
// func (b *bitmap) serialize(w io.Writer) (int, error) {
// func deserialiseBitmapFromReader(r io.Reader) (*bitmap, error) {
// func newTypedColumnFromSchema(schema columnSchema) typedColumn {
// func newColumnStrings(isNullable bool) *columnStrings {
// func newColumnInts(isNullable bool) *columnInts {
// func newColumnFloats() *columnFloats {
// func newColumnBools(isNullable bool) *columnBools {
// func (rc *columnStrings) addValue(s string) error {
// func (rc *columnStrings) nthValue(n int64) string {
// func (rc *columnInts) addValue(s string) error {
// func (rc *columnFloats) addValue(s string) error {
// func (rc *columnBools) addValue(s string) error {
// func (rc *columnStrings) serializeInto(w io.Writer) (int64, error) {
// func deserializeColumn(r io.Reader, dtype dtype) (typedColumn, error) {
// func deserializeColumnStrings(r io.Reader) (*columnStrings, error) {
// func deserializeColumnInts(r io.Reader) (*columnInts, error) {
// func deserializeColumnFloats(r io.Reader) (*columnFloats, error) {
// func deserializeColumnBools(r io.Reader) (*columnBools, error) {
// func (rc *columnInts) serializeInto(w io.Writer) (int64, error) {
// func (rc *columnFloats) serializeInto(w io.Writer) (int64, error) {
// func (rc *columnBools) serializeInto(w io.Writer) (int64, error) {
// func (rc *columnStrings) MarshalJSON() ([]byte, error) {
// func (rc *columnInts) MarshalJSON() ([]byte, error) {
// func (rc *columnFloats) MarshalJSON() ([]byte, error) {
// func (rc *columnBools) MarshalJSON() ([]byte, error) {

// tests for columnNulls
