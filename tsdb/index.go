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

	CreateMeasurementIndexIfNotExists(name []byte) (*Measurement, error)
	Measurement(name []byte) (*Measurement, error)
	Measurements() (Measurements, error)
	MeasurementsByExpr(expr influxql.Expr) (Measurements, bool, error)
	MeasurementsByName(names [][]byte) ([]*Measurement, error)
	MeasurementsByRegex(re *regexp.Regexp) (Measurements, error)
	DropMeasurement(name []byte) error

	CreateSeriesIfNotExists(name []byte, tags models.Tags) error
	DropSeries(keys [][]byte) error

	SeriesN() (uint64, error)
	SeriesSketches() (estimator.Sketch, estimator.Sketch, error)
	MeasurementsSketches() (estimator.Sketch, estimator.Sketch, error)

	Dereference(b []byte)

	TagSets(name []byte, dimensions []string, condition influxql.Expr) ([]*influxql.TagSet, error)
}
