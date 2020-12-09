package column

import "testing"

func TestBasicDates(t *testing.T) {
	tests := []struct {
		input            string
		year, month, day int
		err              error
	}{
		{"2020-02-20", 2020, 2, 20, nil},
		{"0000-12-31", 0, 12, 31, nil},
		// TODO: add invalid dates, overflows etc.
	}

	for _, test := range tests {
		val, err := parseDate(test.input)
		if err != test.err {
			t.Errorf("failed to parse %s as a date with err %v, got %v", test.input, test.err, err)
			continue
		}
		expected := newDate(test.year, test.month, test.day, 0)
		if val != expected {
			t.Errorf("failed to parse %s into %s, got %s instead", test.input, expected, val)
		}

		if val.String() != test.input {
			t.Errorf("failed to roundtrip %v, got %v instead", test.input, val.String())
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
		{"0000-12-31 12:34:56.007890", 0, 12, 31, 12, 34, 56, 7890, true, nil},
		// no roundtrips
		{"2020-12-31 12:34:56.789", 2020, 12, 31, 12, 34, 56, 789, false, nil},
		{"2020-12-31 12:34:56", 2020, 12, 31, 12, 34, 56, 0, false, nil},
		{"2020-12-31T12:34:56.000789", 2020, 12, 31, 12, 34, 56, 789, false, nil},
		{"2020-12-31T12:34:56.789", 2020, 12, 31, 12, 34, 56, 789, false, nil},
		{"2020-12-31T12:34:56", 2020, 12, 31, 12, 34, 56, 0, false, nil},
		// TODO: add invalid datetimes, overflows etc.
	}

	for _, test := range tests {
		val, err := parseDatetime(test.input)
		if err != test.err {
			t.Errorf("failed to parse %s as a date with err %v, got %v", test.input, test.err, err)
			continue
		}
		expected := newDatetime(test.year, test.month, test.day, test.hour, test.minute, test.second, test.microsecond)
		if val != expected {
			t.Errorf("failed to parse %s into %s, got %s instead", test.input, expected, val)
		}

		if test.roundtrip {
			if val.String() != test.input {
				t.Errorf("failed to roundtrip %v, got %v instead", test.input, val.String())
			}
		}
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

// newDatetime
// parseDatetime - test three and six-long microseconds
