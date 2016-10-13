package coordinator

import (
	"regexp"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/tsdb"
)

type Shard interface {
	MeasurementsByRegex(re *regexp.Regexp) []string
	TagSets(measurement string, dimensions []string, condition influxql.Expr) ([]*influxql.TagSet, error)
	FieldDimensions(measurements []string) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error)
	CreateIterator(opt influxql.IteratorOptions) (influxql.Iterator, error)
	CreateSeriesIterator(measurement string, tagSet *influxql.TagSet, opt influxql.IteratorOptions) (influxql.Iterator, error)
}

type localShard struct {
	*tsdb.Shard
}

func (sh localShard) MeasurementsByRegex(re *regexp.Regexp) []string {
	mms := sh.Shard.MeasurementsByRegex(re)
	if len(mms) == 0 {
		return nil
	}

	names := make([]string, len(mms))
	for i, mm := range mms {
		names[i] = mm.Name
	}
	return names
}

func (sh localShard) TagSets(measurement string, dimensions []string, condition influxql.Expr) ([]*influxql.TagSet, error) {
	mm := sh.Shard.MeasurementByName(measurement)
	if mm == nil {
		return nil, nil
	}
	return mm.TagSets(dimensions, condition)
}

func (sh localShard) CreateSeriesIterator(measurement string, tagSet *influxql.TagSet, opt influxql.IteratorOptions) (influxql.Iterator, error) {
	mm := sh.Shard.MeasurementByName(measurement)
	if mm == nil {
		return nil, nil
	}
	return sh.Shard.CreateSeriesIterator(mm, tagSet, opt)
}
