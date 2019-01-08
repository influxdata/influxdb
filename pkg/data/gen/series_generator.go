package gen

import (
	"github.com/influxdata/influxdb/models"
)

type SeriesGenerator interface {
	// Next advances the series generator to the next series key.
	Next() bool

	// Name returns the name of the measurement.
	// The returned value may be modified by a subsequent call to Next.
	Name() []byte

	// Tags returns the tag set.
	// The returned value may be modified by a subsequent call to Next.
	Tags() models.Tags

	// Field returns the name of the field.
	// The returned value may be modified by a subsequent call to Next.
	Field() []byte

	// ValuesGenerator returns a values sequence for the current series.
	ValuesGenerator() ValuesSequence
}

type ValuesSequence interface {
	Reset()
	Next() bool
	Values() Values
}

type Values interface {
	MinTime() int64
	MaxTime() int64
	Encode([]byte) ([]byte, error)
}
