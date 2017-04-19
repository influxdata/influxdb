package internal

import (
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/estimator"
	"github.com/influxdata/influxdb/tsdb/index/tsi1"
)

// File is a mock implementation of a tsi1.File.
type File struct {
	Closef                     func() error
	Pathf                      func() string
	FilterNameTagsf            func(names [][]byte, tagsSlice []models.Tags) ([][]byte, []models.Tags)
	Measurementf               func(name []byte) tsi1.MeasurementElem
	MeasurementIteratorf       func() tsi1.MeasurementIterator
	HasSeriesf                 func(name []byte, tags models.Tags, buf []byte) (exists, tombstoned bool)
	Seriesf                    func(name []byte, tags models.Tags) tsi1.SeriesElem
	SeriesNf                   func() uint64
	TagKeyf                    func(name, key []byte) tsi1.TagKeyElem
	TagKeyIteratorf            func(name []byte) tsi1.TagKeyIterator
	TagValuef                  func(name, key, value []byte) tsi1.TagValueElem
	TagValueIteratorf          func(name, key []byte) tsi1.TagValueIterator
	SeriesIteratorf            func() tsi1.SeriesIterator
	MeasurementSeriesIteratorf func(name []byte) tsi1.SeriesIterator
	TagKeySeriesIteratorf      func(name, key []byte) tsi1.SeriesIterator
	TagValueSeriesIteratorf    func(name, key, value []byte) tsi1.SeriesIterator
	MergeSeriesSketchesf       func(s, t estimator.Sketch) error
	MergeMeasurementsSketchesf func(s, t estimator.Sketch) error
	Retainf                    func()
	Releasef                   func()
}

func (f *File) Close() error { return f.Closef() }
func (f *File) Path() string { return f.Pathf() }
func (f *File) FilterNamesTags(names [][]byte, tagsSlice []models.Tags) ([][]byte, []models.Tags) {
	return f.FilterNameTagsf(names, tagsSlice)
}
func (f *File) Measurement(name []byte) tsi1.MeasurementElem  { return f.Measurementf(name) }
func (f *File) MeasurementIterator() tsi1.MeasurementIterator { return f.MeasurementIteratorf() }
func (f *File) HasSeries(name []byte, tags models.Tags, buf []byte) (exists, tombstoned bool) {
	return f.HasSeriesf(name, tags, buf)
}
func (f *File) Series(name []byte, tags models.Tags) tsi1.SeriesElem { return f.Seriesf(name, tags) }
func (f *File) SeriesN() uint64                                      { return f.SeriesNf() }
func (f *File) TagKey(name, key []byte) tsi1.TagKeyElem              { return f.TagKeyf(name, key) }
func (f *File) TagKeyIterator(name []byte) tsi1.TagKeyIterator       { return f.TagKeyIteratorf(name) }
func (f *File) TagValue(name, key, value []byte) tsi1.TagValueElem {
	return f.TagValuef(name, key, value)
}
func (f *File) TagValueIterator(name, key []byte) tsi1.TagValueIterator {
	return f.TagValueIteratorf(name, key)
}
func (f *File) SeriesIterator() tsi1.SeriesIterator { return f.SeriesIteratorf() }
func (f *File) MeasurementSeriesIterator(name []byte) tsi1.SeriesIterator {
	return f.MeasurementSeriesIteratorf(name)
}
func (f *File) TagKeySeriesIterator(name, key []byte) tsi1.SeriesIterator {
	return f.TagKeySeriesIteratorf(name, key)
}
func (f *File) TagValueSeriesIterator(name, key, value []byte) tsi1.SeriesIterator {
	return f.TagValueSeriesIteratorf(name, key, value)
}
func (f *File) MergeSeriesSketches(s, t estimator.Sketch) error { return f.MergeSeriesSketchesf(s, t) }
func (f *File) MergeMeasurementsSketches(s, t estimator.Sketch) error {
	return f.MergeMeasurementsSketchesf(s, t)
}
func (f *File) Retain()  { f.Retainf() }
func (f *File) Release() { f.Releasef() }
