package tsm1

import (
	"github.com/influxdata/influxdb/v2/tsdb"
)

var TestMmapInitFailOption = func(err error) tsmReaderOption {
	return func(r *TSMReader) {
		r.accessor = &badBlockAccessor{error: err}
	}
}

type badBlockAccessor struct {
	error
	initCalled bool
}

func (b *badBlockAccessor) init() (*indirectIndex, error) {
	b.initCalled = true
	return nil, b.error
}

func (b *badBlockAccessor) read(key []byte, timestamp int64) ([]Value, error) {
	//TODO implement me
	panic("implement me")
}

func (b *badBlockAccessor) readAll(key []byte) ([]Value, error) {
	//TODO implement me
	panic("implement me")
}

func (b *badBlockAccessor) readBlock(entry *IndexEntry, values []Value) ([]Value, error) {
	//TODO implement me
	panic("implement me")
}

func (b *badBlockAccessor) readFloatBlock(entry *IndexEntry, values *[]FloatValue) ([]FloatValue, error) {
	//TODO implement me
	panic("implement me")
}

func (b *badBlockAccessor) readFloatArrayBlock(entry *IndexEntry, values *tsdb.FloatArray) error {
	//TODO implement me
	panic("implement me")
}

func (b *badBlockAccessor) readIntegerBlock(entry *IndexEntry, values *[]IntegerValue) ([]IntegerValue, error) {
	//TODO implement me
	panic("implement me")
}

func (b *badBlockAccessor) readIntegerArrayBlock(entry *IndexEntry, values *tsdb.IntegerArray) error {
	//TODO implement me
	panic("implement me")
}

func (b *badBlockAccessor) readUnsignedBlock(entry *IndexEntry, values *[]UnsignedValue) ([]UnsignedValue, error) {
	//TODO implement me
	panic("implement me")
}

func (b *badBlockAccessor) readUnsignedArrayBlock(entry *IndexEntry, values *tsdb.UnsignedArray) error {
	//TODO implement me
	panic("implement me")
}

func (b *badBlockAccessor) readStringBlock(entry *IndexEntry, values *[]StringValue) ([]StringValue, error) {
	//TODO implement me
	panic("implement me")
}

func (b *badBlockAccessor) readStringArrayBlock(entry *IndexEntry, values *tsdb.StringArray) error {
	//TODO implement me
	panic("implement me")
}

func (b *badBlockAccessor) readBooleanBlock(entry *IndexEntry, values *[]BooleanValue) ([]BooleanValue, error) {
	//TODO implement me
	panic("implement me")
}

func (b *badBlockAccessor) readBooleanArrayBlock(entry *IndexEntry, values *tsdb.BooleanArray) error {
	//TODO implement me
	panic("implement me")
}

func (b *badBlockAccessor) readBytes(entry *IndexEntry, buf []byte) (uint32, []byte, error) {
	//TODO implement me
	panic("implement me")
}

func (b *badBlockAccessor) rename(path string) error {
	//TODO implement me
	panic("implement me")
}

func (b *badBlockAccessor) path() string {
	//TODO implement me
	panic("implement me")
}

func (b *badBlockAccessor) close() error {
	if !b.initCalled {
		panic("close called without an init call")
	}
	b.initCalled = false
	return nil
}

func (b *badBlockAccessor) free() error {
	//TODO implement me
	panic("implement me")
}
