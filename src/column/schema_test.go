package column

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"strconv"
	"testing"
)

func TestDtypeStringer(t *testing.T) {
	tests := []struct {
		Dtype Dtype
		str   string
	}{
		{DtypeInvalid, "invalid"},
		{DtypeNull, "null"},
		{DtypeInt, "int"},
		{DtypeFloat, "float"},
		{DtypeBool, "bool"},
		{DtypeDate, "date"},
		{DtypeDatetime, "datetime"},
	}

	for _, testCase := range tests {
		if testCase.Dtype.String() != testCase.str {
			t.Errorf("expected %+v to stringify to %+v", testCase.Dtype, testCase.str)
		}
		expectedJSON := fmt.Sprintf("\"%v\"", testCase.str)
		marshaled, err := json.Marshal(testCase.Dtype)
		if err != nil {
			t.Fatal(err)
		}
		if expectedJSON != string(marshaled) {
			t.Errorf("expected %+v to JSON marshal into %+v", string(marshaled), expectedJSON)
		}
	}
}

func TestDtypeJSONRoundtrip(t *testing.T) {
	for _, dt := range []Dtype{DtypeInvalid, DtypeNull, DtypeInt, DtypeFloat, DtypeBool, DtypeDate, DtypeString} {
		bt, err := json.Marshal(dt)
		if err != nil {
			t.Error(err)
			continue
		}
		var dt2 Dtype
		if err := json.Unmarshal(bt, &dt2); err != nil {
			t.Error(err)
		}
		if dt != dt2 {
			t.Errorf("Dtype roundtrip failed, expected %+v, got %+v", dt, dt2)
		}
	}
}

func TestBasicTypeInference(t *testing.T) {
	tt := []struct {
		input    []string
		Dtype    Dtype
		nullable bool
	}{
		{
			[]string{"foo", "bar", "baz"},
			DtypeString,
			false,
		},
		{
			[]string{"foo", "bar", "123"},
			DtypeString,
			false,
		},
		{
			[]string{"foo", "bar", ""},
			DtypeString,
			true,
		},
		{
			[]string{"foo", "bar", " "},
			DtypeString,
			false,
		},
		{
			[]string{"1", "2", "3"},
			DtypeInt,
			false,
		},
		{
			[]string{"1", "2", strconv.Itoa(math.MaxInt64), strconv.Itoa(math.MinInt64)},
			DtypeInt,
			false,
		},
		{
			[]string{"1", "2", "9523372036854775807", "-9523372036854775808"}, // beyond int64 (but valid uint64)
			// when we go past int64, we can still use floats to somewhat represent these, though it may be inaccurate
			// consider forcing strings at some point
			DtypeFloat,
			false,
		},
		{
			[]string{"true", ""},
			DtypeBool,
			true,
		},
		{
			[]string{"true", "false", "TRUE"},
			DtypeBool,
			false,
		},
		{
			[]string{"true", "false", "TRUE", "1"},
			DtypeString, // 1/0 should not be booleans (strconv.parseBool does consider them as such)
			false,
		},
		{
			[]string{"true", "false", "TRUE", "0"},
			DtypeString,
			false,
		},
		{
			[]string{"true", "false", "TRue"},
			DtypeString,
			false,
		},
		{
			[]string{"1.23", "1e7", "-2"},
			DtypeFloat,
			false,
		},
		{
			[]string{"2020-02-22", "1987-12-31", "3000-01-12"},
			DtypeDate,
			false,
		},
		{
			[]string{"2020-02-22", "", "3000-01-12"},
			DtypeDate,
			true,
		},
		{
			[]string{"2020-02-22 12:34:56", "1987-12-31 01:02:03", "3000-01-12 00:00:00"},
			DtypeDatetime,
			false,
		},
		{
			[]string{"2020-02-22 12:34:56", "", "3000-01-12 00:00:00"},
			DtypeDatetime,
			true,
		},
		{
			[]string{"2020-02-22T12:34:56", "3000-01-12 00:00:00"},
			DtypeDatetime,
			false,
		},
		{
			[]string{"2020-02-22 12:34:56", "1987-12-31 01:02:03.123", "3000-01-12 00:00:00.123456"},
			DtypeDatetime,
			false,
		},
		// TODO: once we validate values within dates, add something like this
		// {
		// 	[]string{"2020-13-22", "2000-99-99", "2000-01-64"},
		// 	DtypeDate,
		// 	false,
		// },
		{
			[]string{},
			DtypeInvalid,
			true,
		},
		{
			[]string{"", "", ""},
			DtypeNull,
			true,
		},
	}
	for _, test := range tt {
		guesser := NewTypeGuesser()
		for _, val := range test.input {
			guesser.AddValue(val)
		}
		schema := guesser.InferredType()
		if schema.Dtype != test.Dtype {
			log.Fatalf("unexpected type: %v, expecting: %v (data: %v)", schema.Dtype, test.Dtype, test.input)
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
			t.Errorf("only empty strings should be considered null, got \"%+v\"", val)
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
			t.Errorf("expected %+v to parse as an int into %+v, got %+v", test.input, test.val, resp)
		}
	}
}

func TestIntCoercionErrs(t *testing.T) {
	tests := []string{"123 ", "", "1.2", "1e3", "foo"}

	for _, test := range tests {
		_, err := parseInt(test)
		if err == nil {
			t.Errorf("expected %+v to err, but it did not", test)
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
			t.Errorf("expected %+v to parse as a float into %+v, got %+v", test.input, test.val, resp)
		}
	}
}

func TestFloatCoercionErrs(t *testing.T) {
	tests := []string{"123 ", "", "foo", "1e1900000"}

	for _, test := range tests {
		_, err := parseFloat(test)
		if err == nil {
			t.Errorf("expected %+v to err, but it did not", test)
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
			t.Errorf("expected %+v to parse as a bool into %+v, got %+v", test.input, test.val, resp)
		}
	}
}

func TestBoolCoercionErrs(t *testing.T) {
	tests := []string{"true ", "  false", "N", "Y", "1", "0"} // add True and False once we drop it

	for _, test := range tests {
		_, err := parseBool(test)
		if err == nil {
			t.Errorf("expected %+v to err, but it did not", test)
		}
	}
}

func TestBasicTypeGuessing(t *testing.T) {
	tests := []struct {
		str   string
		Dtype Dtype
	}{
		{"123", DtypeInt},
		{"0", DtypeInt},
		{"123.3", DtypeFloat},
		{".3", DtypeFloat},
		{"+0", DtypeInt},
		{"-0", DtypeInt},
		{"true", DtypeBool},
		{"false", DtypeBool},
		{"2020-02-22", DtypeDate},
		{"foo", DtypeString},
		{"", DtypeString}, // we don't do null inference in guessType
	}
	for _, test := range tests {
		if guessType(test.str) != test.Dtype {
			t.Errorf("expected %+v to be guessed as a %+v, but got %+v", test.str, test.Dtype, guessType(test.str))
		}
	}
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

func BenchmarkDateDetection(b *testing.B) {
	n := 1000
	strvals := make([]string, 0, n)
	nbytes := 0
	for j := 0; j < n; j++ {
		val := fmt.Sprintf("%04d-%02d-%02d", rand.Intn(2020), 1+rand.Intn(12), 1+rand.Intn(30))
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

func BenchmarkDatetimeDetection(b *testing.B) {
	n := 1000
	strvals := make([]string, 0, n)
	nbytes := 0
	for j := 0; j < n; j++ {
		val := fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%06d", rand.Intn(2020), 1+rand.Intn(12), 1+rand.Intn(30), rand.Intn(24), rand.Intn(60), rand.Intn(60), rand.Intn(1000000))
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
