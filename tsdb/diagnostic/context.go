package diagnostic

import "time"

type Context interface {
	ShardContext
	SeriesCardinalityError(err error)
	MeasurementsCardinalityError(err error)
	UsingDataDir(path string)
	NotADatabaseDir(name string)
	SkippingRetentionPolicyDir(name string)
	ShardOpened(path string, dur time.Duration)
	ShardOpenError(err error)
	FreeColdShardError(err error)
	MeasurementNamesByExprError(err error)
	WarnMaxValuesPerTagLimitExceeded(perc, n, max int, db string, name, tag []byte)
}

type ShardContext interface {
	AttachEngine(name string, engine interface{})
	AttachIndex(name string, index interface{})
}
