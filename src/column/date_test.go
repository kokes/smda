package column

import (
	"fmt"
	"testing"
	"time"
)

func TestBasicDates(t *testing.T) {
	tests := []struct {
		year, month, day int
		err              error
	}{
		{2020, 2, 20, nil},
		{1987, 12, 31, nil},
		{0, 12, 31, nil},
		// leap years
		{2021, 2, 29, errInvalidDate},
		{1600, 2, 29, nil},
		{2000, 2, 29, nil},
		{1700, 2, 29, errInvalidDate},
		{1800, 2, 29, errInvalidDate},
		{1900, 2, 29, errInvalidDate},
		// invalid dates
		{2021, 1, 32, errInvalidDate},
		{2021, 2, 30, errInvalidDate},
		{2021, 3, 32, errInvalidDate},
		{2021, 4, 31, errInvalidDate},
		{2021, 5, 32, errInvalidDate},
		{2021, 6, 31, errInvalidDate},
		{2021, 7, 32, errInvalidDate},
		{2021, 8, 32, errInvalidDate},
		{2021, 9, 31, errInvalidDate},
		{2021, 10, 32, errInvalidDate},
		{2021, 11, 31, errInvalidDate},
		{2021, 12, 32, errInvalidDate},
		{2021, 4, 0, errInvalidDate},
		{2021, 0, 30, errInvalidDate},
	}

	for _, test := range tests {
		input := fmt.Sprintf("%04d-%02d-%02d", test.year, test.month, test.day)
		val, err := parseDate(input)
		if err != test.err {
			t.Errorf("failed to parse %s as a date with err %+v, got %+v", input, test.err, err)
			continue
		}
		expected, err := newDate(test.year, test.month, test.day, 0)
		if err != test.err {
			t.Errorf("expected %v to return with %v, got %v instead", input, test.err, err)
			continue
		}
		if test.err == nil && val != expected {
			t.Errorf("failed to parse %s into %s, got %s instead", input, expected, val)
			continue
		}

		if test.err == nil && val.String() != input {
			t.Errorf("failed to roundtrip %+v, got %+v instead", input, val.String())
		}
	}
}

func TestBasicDatetimes(t *testing.T) {
	tests := []struct {
		input                                               string
		year, month, day, hour, minute, second, microsecond int
		roundtrip                                           bool
		err                                                 error
	}{
		{"2020-02-20 01:02:03.000004", 2020, 2, 20, 1, 2, 3, 4, true, nil},
		{"2020-12-20 01:02:03.000004", 2020, 12, 20, 1, 2, 3, 4, true, nil},
		{"2021-09-08 01:02:03.000004", 2021, 9, 8, 1, 2, 3, 4, true, nil},
		{"0000-12-31 12:34:56.007890", 0, 12, 31, 12, 34, 56, 7890, true, nil},
		// no roundtrips
		{"2020-12-31 12:34:56.789", 2020, 12, 31, 12, 34, 56, 789, false, nil},
		{"2020-12-31 12:34:56", 2020, 12, 31, 12, 34, 56, 0, false, nil},
		{"2020-12-31T12:34:56.000789", 2020, 12, 31, 12, 34, 56, 789, false, nil},
		{"2020-12-31T12:34:56.789", 2020, 12, 31, 12, 34, 56, 789, false, nil},
		{"2020-12-31T12:34:56", 2020, 12, 31, 12, 34, 56, 0, false, nil},
		// leap years
		{"1600-02-29 00:01:03", 1600, 2, 29, 0, 1, 3, 0, false, nil},
		{"2000-02-29 00:01:03", 2000, 2, 29, 0, 1, 3, 0, false, nil},
		{"2021-02-29 00:01:03", 0, 0, 0, 0, 0, 0, 0, false, errInvalidDate},
		{"1700-02-29 00:01:03", 0, 0, 0, 0, 0, 0, 0, false, errInvalidDate},
		{"1800-02-29 00:01:03", 0, 0, 0, 0, 0, 0, 0, false, errInvalidDate},
		{"1900-02-29 00:01:03", 0, 0, 0, 0, 0, 0, 0, false, errInvalidDate},
	}

	for _, test := range tests {
		val, err := parseDatetime(test.input)
		if err != test.err {
			t.Errorf("failed to parse %s as a datetime with err %+v, got %+v", test.input, test.err, err)
			continue
		}
		expected, err := newDatetime(test.year, test.month, test.day, test.hour, test.minute, test.second, test.microsecond)
		if err != test.err {
			t.Errorf("failed to parse %s as a date with err %+v, got %+v", test.input, test.err, err)
			continue
		}
		if val != expected {
			t.Errorf("failed to parse %s into %s, got %s instead", test.input, expected, val)
		}

		if test.roundtrip {
			if val.String() != test.input {
				t.Errorf("failed to roundtrip %+v, got %+v instead", test.input, val.String())
			}
		}
	}
}

func TestNativeConversion(t *testing.T) {
	loc := time.Now().Location()
	tests := []struct {
		input    time.Time
		expected string
	}{
		{time.Date(2021, 9, 3, 12, 34, 56, 0, loc), "2021-09-03 12:34:56.000000"},
		{time.Date(2021, 9, 3, 12, 34, 56, 1000, loc), "2021-09-03 12:34:56.000001"},
		{time.Date(2021, 9, 3, 12, 34, 56, 123456, loc), "2021-09-03 12:34:56.000123"},
		{time.Date(2021, 9, 3, 12, 34, 56, 123456789, loc), "2021-09-03 12:34:56.123456"},
	}

	for _, test := range tests {
		val, err := newDatetimeFromNative(test.input)
		if err != nil {
			t.Errorf("failed to convert time.Time(%s) into a datetime (%v)", test.input, err)
			continue
		}
		if val.String() != test.expected {
			t.Errorf("expected time.Time(%v) to be converted to %v, got %v instead", test.input, test.expected, val.String())
		}
	}
}

func BenchmarkDateParsing(b *testing.B) {
	data := []string{"2020-01-01", "2020-12-12", "1950-04-30"}
	var nbytes int64
	for _, s := range data {
		nbytes += int64(len(s))
	}

	b.ResetTimer()
	b.SetBytes(nbytes)
	for j := 0; j < b.N; j++ {
		for _, s := range data {
			if _, err := parseDate(s); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkDatetimeParsing(b *testing.B) {
	tests := []struct {
		name string
		data []string
	}{
		{"seconds", []string{"2020-02-20 12:34:56", "1902-05-30 12:22:06", "1900-01-01 00:00:00"}},
		{"milis", []string{"2020-02-20 12:34:56.000", "1902-05-30 12:22:06.123", "1900-01-01 00:00:00.999"}},
		{"micros", []string{"2020-02-20 12:34:56.000000", "1902-05-30 12:22:06.000123", "1900-01-01 00:00:00.999123"}},
	}
	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			var nbytes int64
			for _, s := range test.data {
				nbytes += int64(len(s))
			}

			b.ResetTimer()
			b.SetBytes(nbytes)
			for j := 0; j < b.N; j++ {
				for _, s := range test.data {
					if _, err := parseDatetime(s); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

// func newDate(year, month, day int) date {
// func (d date) Year() int  { return int(d >> 10) }
// func (d date) Month() int { return int(d >> 5 & (1<<5 - 1)) }
// func (d date) Day() int   { return int(d & (1<<5 - 1)) }
// func parseDate(s string) (date, error) {
// func DatesEqual(a, b date) bool {
// func DatesNotEqual(a, b date) bool {
// func DatesLessThan(a, b date) bool {
// func DatesLessThanEqual(a, b date) bool {
// func DatesGreaterThan(a, b date) bool {
// func DatesGreaterThanEqual(a, b date) bool {
