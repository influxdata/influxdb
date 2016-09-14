package tsdb

import (
	"regexp"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
)

type Index interface {
	Open() error
	Close() error

	CreateMeasurementIndexIfNotExists(name string) (*Measurement, error)
	Measurement(name string) (*Measurement, error)
	Measurements() (Measurements, error)
	MeasurementsByExpr(expr influxql.Expr) (Measurements, bool, error)
	MeasurementsByName(names []string) ([]*Measurement, error)
	MeasurementsByRegex(re *regexp.Regexp) (Measurements, error)
	DropMeasurement(name string) error

	CreateSeriesIndexIfNotExists(measurment string, series *Series) (*Series, error)
	Series(key string) (*Series, error)
	DropSeries(keys []string) error

	SeriesN() (int64, error)

	Statistics(tags map[string]string) []models.Statistic
	TagsForSeries(key string) (models.Tags, error)
}
