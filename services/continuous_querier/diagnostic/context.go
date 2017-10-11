package diagnostic

import "time"

type Context interface {
	Starting()
	Closing()
	RunningByRequest(now time.Time)
	ExecuteContinuousQuery(name string, start, end time.Time)
	ExecuteContinuousQueryError(query string, err error)
	FinishContinuousQuery(name string, written int64, start, end time.Time, dur time.Duration)
}
