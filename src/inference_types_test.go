package smda

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"strconv"
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
		{"True", true}, // we should drop this impl at some point
		{"False", false},
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
	tests := []string{"true ", "  false", "N", "Y"} // add True and False once we drop it

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

// func (db *Database) inferTypes(ds *Dataset) ([]columnSchema, error) {
