package internal

import (
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/bloom"
	"github.com/influxdata/influxdb/pkg/estimator"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/index/tsi1"
)

// File is a mock implementation of a tsi1.File.
type File struct {
	Closef                       func() error
	Pathf                        func() string
	IDf                          func() int
	Levelf                       func() int
	Measurementf                 func(name []byte) tsi1.MeasurementElem
	MeasurementIteratorf         func() tsi1.MeasurementIterator
	HasSeriesf                   func(name []byte, tags models.Tags, buf []byte) (exists, tombstoned bool)
	TagKeyf                      func(name, key []byte) tsi1.TagKeyElem
	TagKeyIteratorf              func(name []byte) tsi1.TagKeyIterator
	TagValuef                    func(name, key, value []byte) tsi1.TagValueElem
	TagValueIteratorf            func(name, key []byte) tsi1.TagValueIterator
	SeriesIDIteratorf            func() tsdb.SeriesIDIterator
	MeasurementSeriesIDIteratorf func(name []byte) tsdb.SeriesIDIterator
	TagKeySeriesIDIteratorf      func(name, key []byte) tsdb.SeriesIDIterator
	TagValueSeriesIDIteratorf    func(name, key, value []byte) tsdb.SeriesIDIterator
	MergeSeriesSketchesf         func(s, t estimator.Sketch) error
	MergeMeasurementsSketchesf   func(s, t estimator.Sketch) error
	Retainf                      func()
	Releasef                     func()
	Filterf                      func() *bloom.Filter
}

func (f *File) Close() error                                  { return f.Closef() }
func (f *File) Path() string                                  { return f.Pathf() }
func (f *File) ID() int                                       { return f.IDf() }
func (f *File) Level() int                                    { return f.Levelf() }
func (f *File) Measurement(name []byte) tsi1.MeasurementElem  { return f.Measurementf(name) }
func (f *File) MeasurementIterator() tsi1.MeasurementIterator { return f.MeasurementIteratorf() }
func (f *File) HasSeries(name []byte, tags models.Tags, buf []byte) (exists, tombstoned bool) {
	return f.HasSeriesf(name, tags, buf)
}
func (f *File) TagKey(name, key []byte) tsi1.TagKeyElem        { return f.TagKeyf(name, key) }
func (f *File) TagKeyIterator(name []byte) tsi1.TagKeyIterator { return f.TagKeyIteratorf(name) }

func (f *File) TagValue(name, key, value []byte) tsi1.TagValueElem {
	return f.TagValuef(name, key, value)
}
func (f *File) TagValueIterator(name, key []byte) tsi1.TagValueIterator {
	return f.TagValueIteratorf(name, key)
}
func (f *File) SeriesIDIterator() tsdb.SeriesIDIterator { return f.SeriesIDIteratorf() }
func (f *File) MeasurementSeriesIDIterator(name []byte) tsdb.SeriesIDIterator {
	return f.MeasurementSeriesIDIteratorf(name)
}
func (f *File) TagKeySeriesIDIterator(name, key []byte) tsdb.SeriesIDIterator {
	return f.TagKeySeriesIDIteratorf(name, key)
}
func (f *File) TagValueSeriesIDIterator(name, key, value []byte) tsdb.SeriesIDIterator {
	return f.TagValueSeriesIDIteratorf(name, key, value)
}
func (f *File) MergeSeriesSketches(s, t estimator.Sketch) error { return f.MergeSeriesSketchesf(s, t) }
func (f *File) MergeMeasurementsSketches(s, t estimator.Sketch) error {
	return f.MergeMeasurementsSketchesf(s, t)
}
func (f *File) Retain()               { f.Retainf() }
func (f *File) Release()              { f.Releasef() }
func (f *File) Filter() *bloom.Filter { return f.Filterf() }
