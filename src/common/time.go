package common

import (
	"time"
)

type EnhancedTime time.Time

func TimeToMicroseconds(t time.Time) int64 {
	return t.Unix()*int64(time.Second/time.Microsecond) + int64(t.Nanosecond())/int64(time.Microsecond)
}

func MicrosecondsToTime(t int64) time.Time {
	return time.Unix(t/int64(time.Second/time.Microsecond), (t%int64(time.Second/time.Microsecond))*int64(time.Microsecond))
}
