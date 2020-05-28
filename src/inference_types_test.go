package smda

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestDtypeStringer(t *testing.T) {
	tests := []struct {
		dtype dtype
		str   string
	}{
		{dtypeInvalid, "invalid"},
		{dtypeNull, "null"},
		{dtypeInt, "int"},
		{dtypeFloat, "float"},
	}

	for _, testCase := range tests {
		if testCase.dtype.String() != testCase.str {
			t.Errorf("expected %v to stringify to %v", testCase.dtype, testCase.str)
		}
		expectedJSON := fmt.Sprintf("\"%v\"", testCase.str)
		marshaled, err := json.Marshal(testCase.dtype)
		if err != nil {
			t.Fatal(err)
		}
		if expectedJSON != string(marshaled) {
			t.Errorf("expected %v to JSON marshal into %v", string(marshaled), expectedJSON)
		}
	}
}

func TestDtypeJSONRoundtrip(t *testing.T) {
	for _, dt := range []dtype{dtypeInvalid, dtypeNull, dtypeInt, dtypeFloat, dtypeBool, dtypeString} {
		bt, err := json.Marshal(dt)
		if err != nil {
			t.Error(err)
			continue
		}
		var dt2 dtype
		if err := json.Unmarshal(bt, &dt2); err != nil {
			t.Error(err)
		}
		if dt != dt2 {
			t.Errorf("dtype roundtrip failed, expected %v, got %v", dt, dt2)
		}
	}
}

func TestBasicTypeInference(t *testing.T) {
	tt := []struct {
		input    []string
		dtype    dtype
		nullable bool
	}{
		{
			[]string{"foo", "bar", "baz"},
			dtypeString,
			false,
		},
		{
			[]string{"foo", "bar", "123"},
			dtypeString,
			false,
		},
		{
			[]string{"foo", "bar", ""},
			dtypeString,
			true,
		},
		{
			[]string{"foo", "bar", " "},
			dtypeString,
			false,
		},
		{
			[]string{"1", "2", "3"},
			dtypeInt,
			false,
		},
		{
			[]string{"1", "2", strconv.Itoa(math.MaxInt64), strconv.Itoa(math.MinInt64)},
			dtypeInt,
			false,
		},
		{
			[]string{"1", "2", "9523372036854775807", "-9523372036854775808"}, // beyond int64 (but valid uint64)
			// when we go past int64, we can still use floats to somewhat represent these, though it may be inaccurate
			// consider forcing strings at some point
			dtypeFloat,
			false,
		},
		{
			[]string{"true", ""},
			dtypeBool,
			true,
		},
		{
			[]string{"true", "false", "TRUE"},
			dtypeBool,
			false,
		},
		{
			[]string{"true", "false", "TRUE", "1"},
			dtypeString, // 1/0 should not be booleans (strconv.parseBool does consider them as such)
			false,
		},
		{
			[]string{"true", "false", "TRUE", "0"},
			dtypeString,
			false,
		},
		{
			[]string{"true", "false", "TRue"},
			dtypeString,
			false,
		},
		{
			[]string{"1.23", "1e7", "-2"},
			dtypeFloat,
			false,
		},
		{
			[]string{},
			dtypeInvalid,
			true,
		},
		{
			[]string{"", "", ""},
			dtypeNull,
			true,
		},
	}
	for _, test := range tt {
		guesser := newTypeGuesser()
		for _, val := range test.input {
			guesser.addValue(val)
		}
		schema := guesser.inferredType()
		if schema.Dtype != test.dtype {
			log.Fatalf("unexpected type: %v, expecting: %v (data: %v)", schema.Dtype, test.dtype, test.input)
		}
		if schema.Nullable != test.nullable {
			log.Fatalf("unexpected nullability: %v, expecting: %v (data: %v)", schema.Nullable, test.nullable, test.input)
		}
	}
}

func TestNullability(t *testing.T) {
	if !isNull("") {
		t.Errorf("an empty string should be considered null")
	}

	// at some point we'll have custom null values, but for now it's just empty strings
	for _, val := range []string{"foo", "bar", " ", "\t", "\n", "-", "NA", "N/A"} {
		if isNull(val) {
			t.Errorf("only empty strings should be considered null, got \"%v\"", val)
		}
	}
}

func TestIntCoercion(t *testing.T) {
	tests := []struct {
		input string
		val   int64
	}{
		{"123", 123},
		{"1900000", 1900000},
		{strconv.Itoa(math.MaxInt64), math.MaxInt64},
		{strconv.Itoa(math.MinInt64), math.MinInt64},
	}

	for _, test := range tests {
		resp, err := parseInt(test.input)
		if err != nil {
			t.Error(err)
		}
		if resp != test.val {
			t.Errorf("expected %v to parse as an int into %v, got %v", test.input, test.val, resp)
		}
	}
}

func TestIntCoercionErrs(t *testing.T) {
	tests := []string{"123 ", "", "1.2", "1e3", "foo"}

	for _, test := range tests {
		_, err := parseInt(test)
		if err == nil {
			t.Errorf("expected %v to err, but it did not", test)
		}
	}
}

func TestFloatCoercion(t *testing.T) {
	tests := []struct {
		input string
		val   float64
	}{
		{"123", 123},
		{"1900000", 1900000},
		{"1e3", 1000},
		{"1.23", 1.23},
		{".3", 0.3},
		{strconv.Itoa(math.MaxInt64), math.MaxInt64},
		{strconv.Itoa(math.MinInt64), math.MinInt64},
		{strconv.FormatFloat(math.MaxInt64, 'E', -1, 64), float64(math.MaxInt64)},
		{strconv.FormatFloat(math.MinInt64, 'E', -1, 64), float64(math.MinInt64)},
		{strconv.FormatFloat(math.MaxFloat64, 'E', -1, 64), math.MaxFloat64},
		{strconv.FormatFloat(math.SmallestNonzeroFloat64, 'E', -1, 64), math.SmallestNonzeroFloat64},
	}

	for _, test := range tests {
		resp, err := parseFloat(test.input)
		if err != nil {
			t.Error(err)
		}
		if resp != test.val {
			t.Errorf("expected %v to parse as a float into %v, got %v", test.input, test.val, resp)
		}
	}
}

func TestFloatCoercionErrs(t *testing.T) {
	tests := []string{"123 ", "", "foo", "1e1900000"}

	for _, test := range tests {
		_, err := parseFloat(test)
		if err == nil {
			t.Errorf("expected %v to err, but it did not", test)
		}
	}
}

func TestBoolCoercion(t *testing.T) {
	tests := []struct {
		input string
		val   bool
	}{
		{"T", true},
		{"F", false},
		{"true", true},
		{"false", false},
		{"TRUE", true},
		{"FALSE", false},
	}

	for _, test := range tests {
		resp, err := parseBool(test.input)
		if err != nil {
			t.Error(err)
		}
		if resp != test.val {
			t.Errorf("expected %v to parse as a bool into %v, got %v", test.input, test.val, resp)
		}
	}
}

func TestBoolCoercionErrs(t *testing.T) {
	tests := []string{"true ", "  false", "N", "Y", "1", "0"} // add True and False once we drop it

	for _, test := range tests {
		_, err := parseBool(test)
		if err == nil {
			t.Errorf("expected %v to err, but it did not", test)
		}
	}
}

func TestBasicTypeGuessing(t *testing.T) {
	tests := []struct {
		str   string
		dtype dtype
	}{
		{"123", dtypeInt},
		{"0", dtypeInt},
		{"123.3", dtypeFloat},
		{".3", dtypeFloat},
		{"+0", dtypeInt},
		{"-0", dtypeInt},
		{"true", dtypeBool},
		{"false", dtypeBool},
		{"foo", dtypeString},
		{"", dtypeString}, // we don't do null inference in guessType
	}
	for _, test := range tests {
		if guessType(test.str) != test.dtype {
			t.Errorf("expected %v to be guessed as a %v, but got %v", test.str, test.dtype, guessType(test.str))
		}
	}
}

func TestDatasetTypeInference(t *testing.T) {
	db, err := NewDatabaseTemp()
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
		cs  []columnSchema
	}{
		{"foo\n1\n2", []columnSchema{{"foo", dtypeInt, false}}},
		{"foo,bar\n1,2\n2,false", []columnSchema{{"foo", dtypeInt, false}, {"bar", dtypeString, false}}},
		{"foo\ntrue\nFALSE", []columnSchema{{"foo", dtypeBool, false}}},
		{"foo,bar\na,b\nc,", []columnSchema{{"foo", dtypeString, false}, {"bar", dtypeString, true}}}, // we do have nullable strings
		{"foo,bar\n1,\n2,3", []columnSchema{{"foo", dtypeInt, false}, {"bar", dtypeInt, true}}},
		{"foo,bar\n1,\n2,", []columnSchema{{"foo", dtypeInt, false}, {"bar", dtypeNull, true}}},
		// the following issues are linked to the fact that encoding/csv skips empty rows (???)
		// {"foo\n\n\n", []columnSchema{{"foo", dtypeNull, true}}}, // this should work, but we keep returning invalid
		// {"foo\ntrue\n", []columnSchema{{"foo", dtypeBool, true}}}, // this should be nullable, but we keep saying it is not
		// {"foo\nfoo\n\ntrue", []columnSchema{{"foo", dtypeBool, true}}}, // this should be nullable, but we keep saying it is not
	}
	for _, dataset := range datasets {
		f, err := ioutil.TempFile("", "")
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(f.Name())
		if err := cacheIncomingFile(strings.NewReader(dataset.raw), f.Name()); err != nil {
			t.Fatal(err)
		}
		cs, err := inferTypes(f.Name(), loadSettings{})
		if err != nil {
			t.Error(err)
			continue
		}
		if !reflect.DeepEqual(cs, dataset.cs) {
			t.Errorf("expecting %v to be inferred as %v, got %v", dataset.raw, dataset.cs, cs)
		}
	}
}

func BenchmarkIntDetection(b *testing.B) {
	n := 1000
	strvals := make([]string, 0, n)
	nbytes := 0
	for j := 0; j < n; j++ {
		val := strconv.Itoa(j)
		nbytes += len(val)
		strvals = append(strvals, val)
	}
	b.ResetTimer()
	for j := 0; j < b.N; j++ {
		for _, el := range strvals {
			guessType(el)
		}
	}
	b.SetBytes(int64(nbytes))
}

func BenchmarkFloatDetection(b *testing.B) {
	n := 1000
	strvals := make([]string, 0, n)
	nbytes := 0
	for j := 0; j < n; j++ {
		fl := rand.Float64()
		val := fmt.Sprintf("%v", fl)
		nbytes += len(val)
		strvals = append(strvals, val)
	}
	b.ResetTimer()
	for j := 0; j < b.N; j++ {
		for _, el := range strvals {
			guessType(el)
		}
	}
	b.SetBytes(int64(nbytes))
}

func BenchmarkBoolDetection(b *testing.B) {
	n := 1000
	strvals := make([]string, 0, n)
	nbytes := 0
	for j := 0; j < n; j++ {
		rnd := rand.Intn(2)
		val := "true"
		if rnd == 1 {
			val = "false"
		}
		nbytes += len(val)
		strvals = append(strvals, val)
	}
	b.ResetTimer()
	for j := 0; j < b.N; j++ {
		for _, el := range strvals {
			guessType(el)
		}
	}
	b.SetBytes(int64(nbytes))
}

func BenchmarkStringDetection(b *testing.B) {
	n := 1000
	strvals := make([]string, 0, n)
	nbytes := 0
	for j := 0; j < n; j++ {
		val := strconv.Itoa(j) + "foo"
		nbytes += len(val)
		strvals = append(strvals, val)
	}
	b.ResetTimer()
	for j := 0; j < b.N; j++ {
		for _, el := range strvals {
			guessType(el)
		}
	}
	b.SetBytes(int64(nbytes))
}

func TestContainsDigit(t *testing.T) {
	trues := []string{"1", "+2", "-0", ".5", "123", "foo123"}
	falses := []string{"", "abc", "foobar", ".", "infty", "nan"}

	for _, val := range trues {
		if !containsDigit(val) {
			t.Errorf("expected %v to contain a digit", val)
		}
	}
	for _, val := range falses {
		if containsDigit(val) {
			t.Errorf("expected %v not to contain a digit", val)
		}
	}
}
