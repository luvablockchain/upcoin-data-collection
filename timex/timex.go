package timex

import (
	"time"
)

var unixTs = time.Unix(0, 0).UTC()

var firstWeekOfUnixBegin = unixTs.AddDate(0, 0, int(time.Monday)-int(unixTs.Weekday()))

func MinutesSinceUnix(t time.Time) int {
	t = t.UTC()
	return int(t.Sub(unixTs).Nanoseconds() / int64(time.Minute))
}

func HoursSinceUnix(t time.Time) int {
	t = t.UTC()
	return int(t.Sub(unixTs).Nanoseconds() / int64(time.Hour))
}

func DaysSinceUnix(t time.Time) int {
	t = t.UTC()
	return int(t.Sub(unixTs).Nanoseconds() / int64(24*time.Hour))
}

func WeeksSinceUnix(t time.Time) int {
	t = t.UTC()
	return int(t.Sub(firstWeekOfUnixBegin).Nanoseconds() / int64(24*7*time.Hour))
}

func MonthsSinceUnix(t time.Time) int {
	t = t.UTC()
	prevYear, prevMonth, _ := unixTs.Date()
	currYear, currMonth, _ := t.Date()
	return ((currYear-prevYear) * 12) + (int(currMonth)-int(prevMonth))
}
