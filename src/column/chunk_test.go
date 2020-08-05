package column

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/kokes/smda/src/bitmap"
)

func TestBlankColumnInitialisation(t *testing.T) {
	Dtypes := []Dtype{DtypeString, DtypeInt, DtypeFloat, DtypeBool, DtypeNull}
	for _, dt := range Dtypes {
		for _, nullable := range []bool{true, false} {
			schema := Schema{"", dt, nullable}
			NewChunkFromSchema(schema)
		}
	}
}

func TestInvalidColumnInitialisation(t *testing.T) {
	defer func() {
		if err := recover(); err != nil {
			if err != "unknown schema type: invalid" {
				t.Fatalf("expecting an invalid column not to be initialised with an unknown schema error, got %v", err)
			}
		}
	}()
	schema := Schema{"", DtypeInvalid, true}
	NewChunkFromSchema(schema)
}

func TestBasicStringColumn(t *testing.T) {
	tt := [][]string{
		{"foo", "bar", "baz"},
		{},
		{"", "", "", "foo", ""},
		{""},
	}
	for _, vals := range tt {
		nc := newChunkStrings()
		if err := nc.AddValues(vals); err != nil {
			t.Error(err)
		}
		for j, val := range vals {
			got := nc.nthValue(j)
			if got != val {
				t.Errorf("expecting %v, got %v", val, got)
				return
			}
		}

		_, err := nc.MarshalBinary()
		if err != nil {
			t.Error(err)
			return
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
		nc := newChunkInts()
		if err := nc.AddValues(vals); err != nil {
			t.Error(err)
		}

		_, err := nc.MarshalBinary()
		if err != nil {
			t.Error(err)
			return
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
		nc := newChunkFloats()
		if err := nc.AddValues(vals); err != nil {
			t.Error(err)
		}

		_, err := nc.MarshalBinary()
		if err != nil {
			t.Error(err)
			return
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
		nc := newChunkBools()
		if err := nc.AddValues(vals); err != nil {
			t.Error(err)
		}

		_, err := nc.MarshalBinary()
		if err != nil {
			t.Error(err)
			return
		}
	}
}

func TestBoolColumnFromBits(t *testing.T) {
	for length := 3; length < 30; length++ {
		data := make([]uint64, 0, length)
		for j := 0; j < length; j++ {
			data = append(data, rand.Uint64())
		}
		bc := newChunkBoolsFromBits(data, length*64)
		c1 := bc.data.Count()
		data[length/2] = 1234
		data[length-1] = 38484
		c2 := bc.data.Count()

		if c1 != c2 {
			t.Error("modifying the underlying slice after passing it to newChunkBoolsFromBits should not change the chunk")
		}
	}
}

func TestInvalidInts(t *testing.T) {
	tt := []string{"1.", ".1", "1e3"}

	for _, testCase := range tt {
		nc := newChunkInts()
		if err := nc.AddValue(testCase); err == nil {
			t.Errorf("did not expect \"%v\" to not be a valid int", testCase)
		}
	}
}

func TestInvalidFloats(t *testing.T) {
	tt := []string{"1e123003030303", ".e", "123f"}

	for _, testCase := range tt {
		nc := newChunkFloats()
		if err := nc.AddValue(testCase); err == nil {
			t.Errorf("did not expect \"%v\" to not be a valid float", testCase)
		}
	}
}

func TestInvalidBools(t *testing.T) {
	tt := []string{"Y", "N", "YES", "NO", "True", "False", "1", "0"} // add True/False once we stop supporting it

	for _, testCase := range tt {
		nc := newChunkBools()
		if err := nc.AddValue(testCase); err == nil {
			t.Errorf("did not expect \"%v\" to not be a valid bool", testCase)
		}
	}
}

func TestInvalidNulls(t *testing.T) {
	tt := []string{"foo", "bar", "baz"}

	for _, testCase := range tt {
		nc := newChunkNulls()
		if err := nc.AddValue(testCase); err == nil {
			t.Errorf("did not expect \"%v\" to not be a valid null", testCase)
		}
	}
}

func TestColumnLength(t *testing.T) {
	tt := []struct {
		Dtype  Dtype
		vals   []string
		length int
	}{
		{DtypeString, []string{}, 0},
		{DtypeInt, []string{}, 0},
		{DtypeFloat, []string{}, 0},
		{DtypeBool, []string{}, 0},
		{DtypeNull, []string{}, 0},
		{DtypeString, []string{""}, 1},
		{DtypeInt, []string{""}, 1},
		{DtypeFloat, []string{""}, 1},
		{DtypeBool, []string{""}, 1},
		{DtypeNull, []string{""}, 1},
		{DtypeString, []string{"hello", "world"}, 2},
		{DtypeInt, []string{"1", "2"}, 2},
		{DtypeFloat, []string{"1", "nan"}, 2},
		{DtypeBool, []string{"true", "false"}, 2},
		{DtypeNull, []string{"", ""}, 2},
	}

	for _, test := range tt {
		schema := Schema{"", test.Dtype, true}
		col := NewChunkFromSchema(schema)
		col.AddValues(test.vals)
		if col.Len() != test.length {
			t.Errorf("expecting %v to have length of %v, got %v", test.vals, test.length, col.Len())
		}
	}
}

func TestSerialisationRoundtrip(t *testing.T) {
	tests := []struct {
		schema Schema
		vals   []string
	}{
		{Schema{"", DtypeString, true}, []string{"foo", "", "baz"}},
		{Schema{"", DtypeString, false}, []string{"foo", "bar", "baz"}},
		{Schema{"", DtypeString, true}, []string{}},
		{Schema{"", DtypeString, true}, []string{""}},
		{Schema{"", DtypeInt, true}, []string{}},
		{Schema{"", DtypeInt, true}, []string{""}},
		{Schema{"", DtypeFloat, true}, []string{}},
		{Schema{"", DtypeFloat, true}, []string{""}}, // NaNs get a special treatment (DeepEqual doesn't work here)
		{Schema{"", DtypeBool, true}, []string{}},
		{Schema{"", DtypeBool, true}, []string{""}},
		{Schema{"", DtypeNull, true}, []string{}},
		{Schema{"", DtypeNull, true}, []string{""}},
		{Schema{"", DtypeInt, false}, []string{"1", "2", "3"}},
		{Schema{"", DtypeInt, true}, []string{"1", "", "3"}},
		{Schema{"", DtypeFloat, false}, []string{"1", "2", "3"}},
		{Schema{"", DtypeFloat, true}, []string{"1", "", "3"}}, // NaNs again
		{Schema{"", DtypeBool, false}, []string{"t", "f", "t"}},
		{Schema{"", DtypeBool, true}, []string{"t", "", "f"}},
	}
	for _, test := range tests {
		col := NewChunkFromSchema(test.schema)
		if err := col.AddValues(test.vals); err != nil {
			t.Error(err)
		}
		b, err := col.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		col2, err := Deserialize(bytes.NewReader(b), test.schema.Dtype)
		if err != nil {
			t.Fatal(err)
		}

		if !ChunksEqual(col, col2) {
			t.Errorf("expecting %v, got %v", col, col2)
		}
	}
}

func TestSerialisationUnsupportedTypes(t *testing.T) {
	defer func() {
		if err := recover(); err != "unsupported Dtype: invalid" {
			t.Fatal(err)
		}
	}()
	unsupported := []Dtype{DtypeInvalid}

	for _, dt := range unsupported {
		Deserialize(strings.NewReader(""), dt)
	}
}

func TestJSONMarshaling(t *testing.T) {
	tests := []struct {
		rc       Chunk // use NewChunkFromSchema instead
		values   []string
		expected string
	}{
		{newChunkBools(), []string{}, "[]"},
		{newChunkBools(), []string{}, "[]"},
		{newChunkBools(), []string{"true", "false"}, "[true,false]"},
		{newChunkBools(), []string{"true", "false"}, "[true,false]"},
		{newChunkBools(), []string{"", "true", "", ""}, "[null,true,null,null]"},
		{newChunkInts(), []string{}, "[]"},
		{newChunkInts(), []string{}, "[]"},
		{newChunkInts(), []string{"123", "456"}, "[123,456]"},
		{newChunkInts(), []string{"123", "456"}, "[123,456]"},
		{newChunkInts(), []string{"123", "", "", "456"}, "[123,null,null,456]"},
		{newChunkFloats(), []string{"123", "456"}, "[123,456]"},
		{newChunkFloats(), []string{"123.456", "456.789"}, "[123.456,456.789]"},
		{newChunkFloats(), []string{"123", "", "456"}, "[123,null,456]"},
		{newChunkFloats(), []string{"123", "", "nan"}, "[123,null,null]"},
		// {newColumnFloats(true), []string{"123", "+infty", "-infty"}, "[123,+infty,-infty]"}, // no infty support yet
		{newChunkStrings(), []string{}, "[]"},
		{newChunkStrings(), []string{}, "[]"},
		{newChunkStrings(), []string{"foo", "bar"}, "[\"foo\",\"bar\"]"},
		{newChunkStrings(), []string{"foo", "bar"}, "[\"foo\",\"bar\"]"},
		{newChunkNulls(), []string{""}, "[null]"},
		{newChunkNulls(), []string{"", "", ""}, "[null,null,null]"},

		// we don't really have nullable strings at this point
		// {newColumnStrings(), []string{"", "bar", ""}, "[null,\"bar\",null]"},
	}
	for _, test := range tests {
		if err := test.rc.AddValues(test.values); err != nil {
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

func TestBasicPruning(t *testing.T) {
	tests := []struct {
		Dtype    Dtype
		nullable bool
		values   []string
		bools    []bool
		expected []string
	}{
		{DtypeBool, false, []string{"true", "false", "true"}, []bool{true, true, true}, []string{"true", "false", "true"}},
		{DtypeBool, false, []string{"true", "false", "true"}, []bool{false, false, false}, nil},
		{DtypeBool, false, []string{"true", "false", "true"}, []bool{false, true, false}, []string{"false"}},

		{DtypeInt, false, []string{"1", "2", "3"}, []bool{false, true, false}, []string{"2"}},
		{DtypeFloat, false, []string{"1.23", "+0", "1e3"}, []bool{false, true, false}, []string{"0"}},
		{DtypeInt, false, []string{"1", "2", "3"}, []bool{true, true, true}, []string{"1", "2", "3"}},
		{DtypeFloat, false, []string{"1.23", "+0", "1e3"}, []bool{true, true, true}, []string{"1.23", "+0", "1e3"}},
		{DtypeString, false, []string{"foo", "bar", "foo"}, []bool{false, true, false}, []string{"bar"}},

		{DtypeNull, false, []string{"", "", ""}, []bool{false, true, false}, []string{""}},
		{DtypeNull, false, []string{"", "", ""}, []bool{true, true, true}, []string{"", "", ""}},

		// nullable columns
		{DtypeInt, true, []string{"1", "", ""}, []bool{false, true, false}, []string{""}},
		{DtypeInt, true, []string{"1", "", ""}, []bool{false, false, false}, nil},
		{DtypeInt, true, []string{"1", "", ""}, []bool{true, true, true}, []string{"1", "", ""}},

		{DtypeBool, true, []string{"true", "", "true"}, []bool{true, true, false}, []string{"t", ""}},
		{DtypeFloat, true, []string{"1.23", "+0", ""}, []bool{false, true, false}, []string{"0"}},
		{DtypeString, true, []string{"foo", "", ""}, []bool{true, true, true}, []string{"foo", "", ""}},

		// not pruning anything by leveraging nil pointers
		{DtypeInt, true, []string{"1", "", ""}, nil, nil},
		{DtypeBool, true, []string{"true", "", "true"}, nil, nil},
		{DtypeFloat, true, []string{"1.23", "+0", ""}, nil, nil},
		{DtypeString, true, []string{"foo", "", ""}, nil, nil},

		{DtypeNull, true, []string{"", "", ""}, nil, nil},
	}
	for _, test := range tests {
		testSchema := Schema{Dtype: test.Dtype, Nullable: test.nullable}
		rc := NewChunkFromSchema(testSchema)
		if err := rc.AddValues(test.values); err != nil {
			t.Error(err)
			continue
		}

		var bm *bitmap.Bitmap
		if test.bools != nil {
			bm = bitmap.NewBitmapFromBools(test.bools)
		}
		pruned := rc.Prune(bm)
		expected := NewChunkFromSchema(testSchema)
		if err := expected.AddValues(test.expected); err != nil {
			t.Error(err)
			continue
		}
		if !reflect.DeepEqual(pruned, expected) {
			t.Errorf("expected that pruning %v using %v would result in %v", test.values, test.bools, test.expected)
		}
	}
}

func TestPruningFailureMisalignment(t *testing.T) {
	tests := []struct {
		Dtype    Dtype
		nullable bool
		values   []string
	}{
		{DtypeBool, false, []string{"true", "false", "true"}},
		{DtypeBool, false, []string{"true", "false", "true"}},
		{DtypeBool, false, []string{"true", "false", "true"}},

		{DtypeInt, false, []string{"1", "2", "3"}},
		{DtypeFloat, false, []string{"1.23", "+0", "1e3"}},
		{DtypeString, false, []string{"foo", "bar", "foo"}},

		{DtypeNull, false, []string{"", "", ""}},
		{DtypeNull, false, []string{"", "", ""}},

		// // nullable columns
		{DtypeInt, true, []string{"1", "", ""}},
		{DtypeInt, true, []string{"1", "", ""}},
		{DtypeInt, true, []string{"1", "", ""}},

		{DtypeBool, true, []string{"true", "", "true"}},
		{DtypeFloat, true, []string{"1.23", "+0", ""}},
		{DtypeString, true, []string{"foo", "", ""}},
	}

	for j, test := range tests {
		testSchema := Schema{Dtype: test.Dtype, Nullable: test.nullable}
		rc := NewChunkFromSchema(testSchema)
		if err := rc.AddValues(test.values); err != nil {
			t.Error(err)
			continue
		}
		t.Run(fmt.Sprintf("pruning with fewer values - %v", j), func(t *testing.T) {
			defer func() {
				if err := recover(); err != "pruning bitmap does not align with the dataset" {
					t.Fatal(err)
				}
			}()
			bm := bitmap.NewBitmap(rc.Len() - 1)
			_ = rc.Prune(bm)
		})

		t.Run(fmt.Sprintf("pruning with more values - %v", j), func(t *testing.T) {
			defer func() {
				if err := recover(); err != "pruning bitmap does not align with the dataset" {
					t.Fatal(err)
				}
			}()
			bm := bitmap.NewBitmap(rc.Len() + 1)
			_ = rc.Prune(bm)
		})
	}
}

func TestAppending(t *testing.T) {
	tests := []struct {
		Dtype    Dtype
		nullable bool
		a        []string
		b        []string
		res      []string
	}{
		{DtypeString, false, []string{"1", "2", "3"}, []string{"4", "5", "6"}, []string{"1", "2", "3", "4", "5", "6"}},
		{DtypeInt, false, []string{"1", "2", "3"}, []string{"4", "5", "6"}, []string{"1", "2", "3", "4", "5", "6"}},
		{DtypeFloat, false, []string{"1", "2", "3"}, []string{"4", "5", "6"}, []string{"1", "2", "3", "4", "5", "6"}},
		{DtypeBool, false, []string{"T", "F", "T"}, []string{"F", "F", "T"}, []string{"T", "F", "T", "F", "F", "T"}},

		// nullable (makes no sense for strings, we don't support them?)
		// {DtypeString, true, []string{"1", "2", "3"}, []string{"4", "5", "6"}, []string{"1", "2", "3", "4", "5", "6"}},
		// {DtypeString, true, []string{"1", "", "3"}, []string{"4", "5", ""}, []string{"1", "", "3", "4", "5", ""}},
		{DtypeInt, true, []string{"1", "", "3"}, []string{"4", "5", ""}, []string{"1", "", "3", "4", "5", ""}},
		// NaNs in []float64 -> custom treatment
		{DtypeFloat, true, []string{"1", "", "3"}, []string{"4", "5", ""}, []string{"1", "", "3", "4", "5", ""}},
		{DtypeBool, true, []string{"", "", ""}, []string{"F", "F", ""}, []string{"", "", "", "F", "F", ""}},
	}
	for _, test := range tests {
		rc := NewChunkFromSchema(Schema{Dtype: test.Dtype, Nullable: test.nullable})
		nrc := NewChunkFromSchema(Schema{Dtype: test.Dtype, Nullable: test.nullable})
		rrc := NewChunkFromSchema(Schema{Dtype: test.Dtype, Nullable: test.nullable})

		if err := rc.AddValues(test.a); err != nil {
			t.Error(err)
		}
		if err := nrc.AddValues(test.b); err != nil {
			t.Error(err)
		}
		if err := rrc.AddValues(test.res); err != nil {
			t.Error(err)
		}
		if err := rc.Append(nrc); err != nil {
			t.Error(err)
		}
		if !ChunksEqual(rc, rrc) {
			// fmt.Println(rc.(*ChunkFloats).nullability)
			// fmt.Println(rrc.(*ChunkFloats).nullability)
			fmt.Println(rc, rrc)

			t.Errorf("expected that %v plus %v results in %v", test.a, test.b, test.res)
		}

	}
}

func TestAppendTypeMismatch(t *testing.T) {
	Dtypes := []Dtype{DtypeString, DtypeInt, DtypeFloat, DtypeBool, DtypeNull}

	for _, dt1 := range Dtypes {
		for _, dt2 := range Dtypes {
			if dt1 == dt2 {
				continue
			}
			col1 := NewChunkFromSchema(Schema{"", dt1, true})
			col2 := NewChunkFromSchema(Schema{"", dt2, true})

			if err := col1.Append(col2); err != errAppendTypeMismatch {
				t.Errorf("expecting a type mismatch in Append to result in errTypeMismatchAppend, got: %v", err)
			}
		}
	}
}

func TestHashing(t *testing.T) {
	tests := []struct {
		Dtype Dtype
		data  []string
	}{
		{DtypeString, []string{}},
		{DtypeInt, []string{}},
		{DtypeFloat, []string{}},
		{DtypeBool, []string{}},
		{DtypeNull, []string{}},
		{DtypeString, []string{"foo", "bar", "baz"}},
		{DtypeInt, []string{"1", "2", "3"}},
		{DtypeFloat, []string{"1", "2", "3"}},
		{DtypeBool, []string{"t", "f", "false"}},
		{DtypeNull, []string{"", "", "", ""}},
		// TODO: nullable strings?
		{DtypeInt, []string{"1", "2", ""}},
		{DtypeFloat, []string{"1", "2", ""}},
		{DtypeBool, []string{"t", "f", ""}},
	}

	for _, test := range tests {
		rc := NewChunkFromSchema(Schema{Dtype: test.Dtype, Nullable: true})
		if err := rc.AddValues(test.data); err != nil {
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
	col := newChunkInts()
	for j := 0; j < n; j++ {
		col.AddValue(strconv.Itoa(j))
	}
	b.ResetTimer()

	// why are we testing allocations? consider moving this before the timer reset (in floats as well)
	hashes := make([]uint64, col.Len())
	for j := 0; j < b.N; j++ {
		col.Hash(hashes)
	}
	b.SetBytes(int64(8 * n))
}

func BenchmarkHashingFloats(b *testing.B) {
	n := 10000
	col := newChunkFloats()
	for j := 0; j < n; j++ {
		col.AddValue(strconv.Itoa(j))
	}
	b.ResetTimer()

	hashes := make([]uint64, col.Len())
	for j := 0; j < b.N; j++ {
		col.Hash(hashes)
	}
	b.SetBytes(int64(8 * n))
}

// tests for columnNulls
// tests for .Dtype()
// TestFilterAndPrune
