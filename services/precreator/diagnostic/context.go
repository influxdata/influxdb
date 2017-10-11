package diagnostic

import "time"

type Context interface {
	Starting(checkInterval, advancedPeriod time.Duration)
	Closing()
	PrecreateError(err error)
}
