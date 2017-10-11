package diagnostic

import "time"

type Context interface {
	Starting()
	AlreadyOpen()
	Closing()
	AlreadyClosed()
	DiagnosticRegistered(name string)

	CreateInternalStorageFailure(db string, err error)
	StoreStatistics(db, rp string, interval time.Duration)
	StoreStatisticsError(err error)
	StatisticsRetrievalFailure(err error)
	DroppingPoint(name string, err error)
	StoreStatisticsDone()
}
