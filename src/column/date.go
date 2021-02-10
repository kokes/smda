package column

import (
	"errors"
	"fmt"
	"strconv"
)

var errInvalidDate = errors.New("date is not valid")
var errInvalidDatetime = errors.New("datetime is not valid")

var dayLimit [12]int = [12]int{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}

const DATE_BYTE_SIZE = 4
const DATETIME_BYTE_SIZE = 8

type date uint32
type datetime uint64

func newDate(year, month, day, hour int) (date, error) {
	if month < 1 || month > 12 {
		return 0, errInvalidDate
	}
	maxDays := dayLimit[month-1]
	if month == 2 && year%4 == 0 && (year%100 != 0 || year%400 == 0) {
		maxDays = 29
	}
	if day < 1 || day > maxDays {
		return 0, errInvalidDate
	}

	var myDate int
	myDate |= year << 14
	myDate |= month << 10
	myDate |= day << 5
	myDate |= hour
	return date(myDate), nil
}

func newDatetime(year, month, day, hour, minute, second, microsecond int) (datetime, error) {
	dateHour, err := newDate(year, month, day, hour)
	if err != nil {
		return 0, err
	}
	timePart := 1e6*(minute*60+second) + microsecond // microseconds in a given hour

	return datetime(uint64(dateHour)<<32 + uint64(timePart)), nil
}

func (d date) Year() int  { return int(d >> 14) }
func (d date) Month() int { return int(d >> 10 & (1<<4 - 1)) }
func (d date) Day() int   { return int(d >> 5 & (1<<5 - 1)) }

func (d date) String() string {
	return fmt.Sprintf("%04d-%02d-%02d", d.Year(), d.Month(), d.Day())
}

func (d date) MarshalJSON() ([]byte, error) {
	val := fmt.Sprintf("\"%s\"", d.String())
	return []byte(val), nil
}

func (dt datetime) Year() int  { return int(dt >> (32 + 14)) }
func (dt datetime) Month() int { return int(dt >> (32 + 10) & (1<<5 - 1)) }
func (dt datetime) Day() int   { return int(dt >> (32 + 5) & (1<<5 - 1)) }
func (dt datetime) Hour() int  { return int(dt >> 32 & (1<<5 - 1)) }

func (dt datetime) Minute() int      { return int(dt&(1<<32-1)/1e6) / 60 }
func (dt datetime) Second() int      { return int(dt&(1<<32-1)/1e6) % 60 }
func (dt datetime) Microsecond() int { return int(dt&(1<<32-1)) % 1e6 }

func (dt datetime) String() string {
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%06d", dt.Year(), dt.Month(), dt.Day(), dt.Hour(), dt.Minute(), dt.Second(), dt.Microsecond())
}

func (dt datetime) MarshalJSON() ([]byte, error) {
	val := fmt.Sprintf("\"%s\"", dt.String())
	return []byte(val), nil
}

// TODO: just like with isNull, support alternative formats via loadSettings
func parseDate(s string) (date, error) {
	if !(len(s) == 10 && s[4] == '-' && s[7] == '-') {
		return 0, errInvalidDate
	}
	year, err := strconv.ParseInt(s[:4], 10, 64)
	if err != nil {
		return 0, err
	}
	month, err := strconv.ParseInt(s[5:7], 10, 64)
	if err != nil {
		return 0, err
	}
	day, err := strconv.ParseInt(s[8:10], 10, 64)
	if err != nil {
		return 0, err
	}

	return newDate(int(year), int(month), int(day), 0)
}

func parseDatetime(s string) (datetime, error) {
	var (
		us  int
		err error
	)
	switch len(s) {
	case 23, 26:
		if s[19] != '.' {
			return 0, errInvalidDatetime
		}
		us, err = strconv.Atoi(s[20:])
		if err != nil {
			return 0, err
		}
		fallthrough
	case 19:
		dt, err := parseDate(s[:10])
		if err != nil {
			return 0, err
		}
		if !(s[10] == ' ' || s[10] == 'T') {
			return 0, errInvalidDatetime
		}
		if !(s[13] == ':' && s[16] == ':') {
			return 0, errInvalidDatetime
		}
		hour, err := strconv.Atoi(s[11:13])
		if err != nil {
			return 0, errInvalidDatetime
		}
		minute, err := strconv.Atoi(s[14:16])
		if err != nil {
			return 0, errInvalidDatetime
		}
		second, err := strconv.Atoi(s[17:19])
		if err != nil {
			return 0, errInvalidDatetime
		}

		return newDatetime(dt.Year(), dt.Month(), dt.Day(), hour, minute, second, us)
	default:
		return 0, errInvalidDatetime
	}
}

func DatesEqual(a, b date) bool {
	return a == b
}
func DatesNotEqual(a, b date) bool {
	return !DatesEqual(a, b)
}
func DatesGreaterThan(a, b date) bool {
	return a > b
}
func DatesGreaterThanEqual(a, b date) bool {
	return a >= b
}
func DatesLessThan(a, b date) bool {
	return DatesGreaterThan(b, a)
}
func DatesLessThanEqual(a, b date) bool {
	return DatesGreaterThanEqual(b, a)
}

func DatetimesEqual(a, b datetime) bool {
	return a == b
}
func DatetimesNotEqual(a, b datetime) bool {
	return !DatetimesEqual(a, b)
}
func DatetimesGreaterThan(a, b datetime) bool {
	return a > b
}
func DatetimesGreaterThanEqual(a, b datetime) bool {
	return a >= b
}
func DatetimesLessThan(a, b datetime) bool {
	return DatetimesGreaterThan(b, a)
}
func DatetimesLessThanEqual(a, b datetime) bool {
	return DatetimesGreaterThanEqual(b, a)
}
