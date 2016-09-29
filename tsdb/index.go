package tsdb

import (
	"regexp"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/estimator"
)

type Index interface {
	Open() error
	Close() error

	CreateMeasurementIndexIfNotExists(name string) (*Measurement, error)
	Measurement(name []byte) (*Measurement, error)
	Measurements() (Measurements, error)
	MeasurementsByExpr(expr influxql.Expr) (Measurements, bool, error)
	MeasurementsByName(names []string) ([]*Measurement, error)
	MeasurementsByRegex(re *regexp.Regexp) (Measurements, error)
	DropMeasurement(name []byte) error

	CreateSeriesIndexIfNotExists(measurment string, series *Series) (*Series, error)
	Series(key []byte) (*Series, error)
	DropSeries(keys []string) error

	SeriesN() (uint64, error)
	SeriesSketches() (estimator.Sketch, estimator.Sketch, error)
	MeasurementsSketches() (estimator.Sketch, estimator.Sketch, error)

	TagsForSeries(key string) (models.Tags, error)
}
