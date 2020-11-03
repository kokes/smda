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
