package upcoin

import (
	"collector/timex"
	"github.com/pkg/errors"
	"time"
)

func getKlineId(open time.Time, interval string) (int64, error) {
	switch interval {
	case "1m":
		return int64(timex.MinutesSinceUnix(open)), nil
	case "3m":
		return int64(timex.MinutesSinceUnix(open) / 3), nil
	case "5m":
		return int64(timex.MinutesSinceUnix(open) / 5), nil
	case "15m":
		return int64(timex.MinutesSinceUnix(open) / 15), nil
	case "30m":
		return int64(timex.MinutesSinceUnix(open) / 30), nil
	case "1h":
		return int64(timex.HoursSinceUnix(open)), nil
	case "2h":
		return int64(timex.HoursSinceUnix(open) / 2), nil
	case "4h":
		return int64(timex.HoursSinceUnix(open) / 4), nil
	case "6h":
		return int64(timex.HoursSinceUnix(open) / 6), nil
	case "8h":
		return int64(timex.HoursSinceUnix(open) / 8), nil
	case "12h":
		return int64(timex.HoursSinceUnix(open) / 12), nil
	case "1d":
		return int64(timex.DaysSinceUnix(open)), nil
	case "3d":
		return int64(timex.DaysSinceUnix(open) / 3), nil
	case "1w":
		return int64(timex.WeeksSinceUnix(open)), nil
	case "1M":
		return int64(timex.MonthsSinceUnix(open)), nil
	default:
		return 0, errors.New("interval is not valid")
	}
}

func countDayInMonth(t time.Time) (int, error) {
	currMonthBegin := beginOfMonth(t)
	nextMonthBegin := currMonthBegin.AddDate(0, 1, 0)
	dayCount := nextMonthBegin.Sub(currMonthBegin).Hours()/(24*time.Hour).Hours()
	if dayCount == float64(int(dayCount)) {
		return int(dayCount), nil
	} else {
		return -1, errors.New("failed to count, day count is not natural number")
	}
}

func beginOfMonth(t time.Time) time.Time {
	y, m, _ := t.Date()
	return time.Date(y, m, 1, 0, 0, 0, 0, t.Location())
}

func endOfMonth(t time.Time) time.Time {
	return beginOfMonth(t).AddDate(0, 1, 0).Add(-time.Nanosecond)
}
