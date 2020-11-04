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
		if !(val.Year() == test.year && val.Month() == test.month && val.Day() == test.day) {
			t.Errorf("failed to parse %s into %04d-%02d-%02d, got %04d-%02d-%02d instead", test.input, test.year, test.month, test.day, val.Year(), val.Month(), val.Day())
		}

		if val.String() != test.input {
			t.Errorf("failed to roundtrip %v, got %v instead", test.input, val.String())
		}
	}
}

// func newDate(year, month, day int) date {
// func (d date) Year() int  { return int(d >> 10) }
// func (d date) Month() int { return int(d >> 5 & (1<<5 - 1)) }
// func (d date) Day() int   { return int(d & (1<<5 - 1)) }
// func (d date) String() string {
// func parseDate(s string) (date, error) {
// func DatesEqual(a, b date) bool {
// func DatesNotEqual(a, b date) bool {
// func DatesLessThan(a, b date) bool {
// func DatesLessThanEqual(a, b date) bool {
// func DatesGreaterThan(a, b date) bool {
// func DatesGreaterThanEqual(a, b date) bool {
