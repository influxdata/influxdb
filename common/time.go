package common

import (
	"time"
)

func TimeToMicroseconds(t time.Time) int64 {
	return t.Unix()*int64(time.Second/time.Microsecond) + int64(t.Nanosecond())/int64(time.Microsecond)
}

func TimeFromMicroseconds(t int64) time.Time {
	t *= 1000
	return time.Unix(t/int64(time.Second), t%int64(time.Second))
}
