package values

import (
	"time"
)

type Time int64
type Duration int64

const (
	fixedWidthTimeFmt = "2006-01-02T15:04:05.000000000Z"
)

func ConvertTime(t time.Time) Time {
	return Time(t.UnixNano())
}

func (t Time) Round(d Duration) Time {
	if d <= 0 {
		return t
	}
	r := remainder(t, d)
	if lessThanHalf(r, d) {
		return t - Time(r)
	}
	return t + Time(d-r)
}

func (t Time) Truncate(d Duration) Time {
	if d <= 0 {
		return t
	}
	r := remainder(t, d)
	return t - Time(r)
}

func (t Time) Add(d Duration) Time {
	return t + Time(d)
}

// lessThanHalf reports whether x+x < y but avoids overflow,
// assuming x and y are both positive (Duration is signed).
func lessThanHalf(x, y Duration) bool {
	return uint64(x)+uint64(x) < uint64(y)
}

// remainder divides t by d and returns the remainder.
func remainder(t Time, d Duration) (r Duration) {
	return Duration(int64(t) % int64(d))
}

func (t Time) String() string {
	return t.Time().Format(fixedWidthTimeFmt)
}

func ParseTime(s string) (Time, error) {
	t, err := time.Parse(fixedWidthTimeFmt, s)
	if err != nil {
		return 0, err
	}
	return ConvertTime(t), nil
}

func (t Time) Time() time.Time {
	return time.Unix(0, int64(t)).UTC()
}

func (d Duration) Duration() time.Duration {
	return time.Duration(d)
}
func (d Duration) String() string {
	return time.Duration(d).String()
}
func ParseDuration(s string) (Duration, error) {
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, err
	}
	return Duration(d), nil
}
