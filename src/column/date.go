package column

import (
	"fmt"
	"strconv"
)

const DATE_BYTE_SIZE = 4
const DATETIME_BYTE_SIZE = 8

type date uint32
type datetime uint64

func newDate(year, month, day, hour int) date {
	// OPTIM: if we initialise this as an int and then shift natively
	// and only convert upon return, will we gain anything?
	// TODO: validation?
	var myDate uint32
	myDate |= uint32(year << 14)
	myDate |= uint32(month << 10)
	myDate |= uint32(day << 5)
	myDate |= uint32(hour)
	return date(myDate)
}

func newDatetime(year, month, day, hour, minute, second, microsecond int) datetime {
	dateHour := newDate(year, month, day, hour)
	timePart := 1e6*(minute*60+second) + microsecond // microseconds in a given hour

	return datetime(uint64(dateHour)<<32 + uint64(timePart))
}

func (d date) Year() int  { return int(d >> 14) }
func (d date) Month() int { return int(d >> 10 & (1<<5 - 1)) }
func (d date) Day() int   { return int(d >> 5 & (1<<5 - 1)) }

func (d date) String() string {
	return fmt.Sprintf("%04d-%02d-%02d", d.Year(), d.Month(), d.Day())
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

// TODO: just like with isNull, support alternative formats via loadSettings
func parseDate(s string) (date, error) {
	if !(len(s) == 10 && s[4] == '-' && s[7] == '-') {
		return 0, errNotaDate
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

	return newDate(int(year), int(month), int(day), 0), nil
}

func parseDatetime(s string) (datetime, error) {
	var (
		us  int
		err error
	)
	switch len(s) {
	case 23, 26:
		if s[19] != '.' {
			return 0, errNotaDatetime
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
			return 0, errNotaDatetime
		}
		if !(s[13] == ':' && s[16] == ':') {
			return 0, errNotaDatetime
		}
		hour, err := strconv.Atoi(s[11:13])
		if err != nil {
			return 0, errNotaDatetime
		}
		minute, err := strconv.Atoi(s[14:16])
		if err != nil {
			return 0, errNotaDatetime
		}
		second, err := strconv.Atoi(s[17:19])
		if err != nil {
			return 0, errNotaDatetime
		}

		return newDatetime(dt.Year(), dt.Month(), dt.Day(), hour, minute, second, us), nil
	default:
		return 0, errNotaDatetime
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
