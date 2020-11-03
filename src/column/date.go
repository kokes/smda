package column

import (
	"fmt"
	"strconv"
)

// ARCH/TODO: consider date(time)s. We'll have to think about AddValue and whether we want to autodetect them
//  		  as that can be very expensive and it's not usually done
// type date uint32
// // we could reserve 5 bits at the end for hour, so that datetime can
// // be datehour + microseconds in an hour (log2(1000*1000*60*60) < 32)

type date uint32

func newDate(year, month, day int) date {
	// OPTIM: if we initialise this as an int and then shift natively
	// and only convert upon return, will we gain anything?
	// TODO: validation?
	var myDate uint32
	myDate |= uint32(year << 10)
	myDate |= uint32(month << 5) // can be four bits
	myDate |= uint32(day)
	return date(myDate)
}

func (d date) Year() int  { return int(d >> 10) }
func (d date) Month() int { return int(d >> 5 & (1<<5 - 1)) }
func (d date) Day() int   { return int(d & (1<<5 - 1)) }

func (d date) String() string {
	return fmt.Sprintf("%04d-%02d-%02d", d.Year(), d.Month(), d.Day())
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

	return newDate(int(year), int(month), int(day)), nil
}
