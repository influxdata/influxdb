package datastore

import (
	"bytes"
	"encoding/binary"
	"time"

	"code.google.com/p/goprotobuf/proto"
	"code.google.com/p/log4go"
	"github.com/influxdb/influxdb/datastore/storage"
	"github.com/influxdb/influxdb/metastore"
	"github.com/influxdb/influxdb/protocol"
)

// PointIterator takes a slice of iterators and their corresponding
// fields and turn it into a point iterator, i.e. an iterator that
// yields whole points instead of column values.
type PointIterator struct {
	itrs               []storage.Iterator
	fields             []*metastore.Field
	startTime, endTime []byte
	rawColumnValues    []rawColumnValue
	valid              bool
	err                error
	point              *protocol.Point
	asc                bool
}

func NewPointIterator(itrs []storage.Iterator, fields []*metastore.Field, startTime, endTime time.Time, asc bool) PointIterator {
	pi := PointIterator{
		valid:           true,
		err:             nil,
		itrs:            itrs,
		fields:          fields,
		rawColumnValues: make([]rawColumnValue, len(fields)),
		startTime:       byteArrayForTime(startTime),
		endTime:         byteArrayForTime(endTime),
		asc:             asc,
	}

	// seek to the first point
	pi.Next()
	return pi
}

func (pi *PointIterator) Next() {
	buffer := bytes.NewBuffer(nil)
	valueBuffer := proto.NewBuffer(nil)
	pi.valid = false
	pi.point = &protocol.Point{Values: make([]*protocol.FieldValue, len(pi.fields))}

	err := pi.getIteratorNextValue()
	if err != nil {
		pi.setError(err)
		return
	}

	var pointTimeRaw []byte
	var pointSequenceRaw []byte
	// choose the highest (or lowest in case of ascending queries) timestamp
	// and sequence number. that will become the timestamp and sequence of
	// the next point.
	for _, value := range pi.rawColumnValues {
		if value.value == nil {
			continue
		}

		pointTimeRaw, pointSequenceRaw = value.updatePointTimeAndSequence(pointTimeRaw, pointSequenceRaw, pi.asc)
	}

	for i, iterator := range pi.itrs {
		// if the value is nil or doesn't match the point's timestamp and sequence number
		// then skip it
		if pi.rawColumnValues[i].value == nil ||
			!bytes.Equal(pi.rawColumnValues[i].time, pointTimeRaw) ||
			!bytes.Equal(pi.rawColumnValues[i].sequence, pointSequenceRaw) {

			pi.point.Values[i] = &protocol.FieldValue{IsNull: &TRUE}
			continue
		}

		// if we emitted at lease one column, then we should keep
		// trying to get more points
		pi.valid = true

		// advance the iterator to read a new value in the next iteration
		if pi.asc {
			iterator.Next()
		} else {
			iterator.Prev()
		}

		fv := &protocol.FieldValue{}
		valueBuffer.SetBuf(pi.rawColumnValues[i].value)
		err := valueBuffer.Unmarshal(fv)
		if err != nil {
			log4go.Error("Error while running query: %s", err)
			pi.setError(err)
			return
		}
		pi.point.Values[i] = fv
		pi.rawColumnValues[i].value = nil
	}

	var sequence uint64
	var t uint64

	// set the point sequence number and timestamp
	buffer.Reset()
	buffer.Write(pointSequenceRaw)
	binary.Read(buffer, binary.BigEndian, &sequence)
	buffer.Reset()
	buffer.Write(pointTimeRaw)
	binary.Read(buffer, binary.BigEndian, &t)

	time := convertUintTimestampToInt64(t)
	pi.point.SetTimestampInMicroseconds(time)
	pi.point.SequenceNumber = &sequence
}

func (pi *PointIterator) getIteratorNextValue() error {
	for i, it := range pi.itrs {
		if pi.rawColumnValues[i].value != nil || !it.Valid() {
			if err := it.Error(); err != nil {
				return err
			}
			continue
		}

		key := it.Key()
		if len(key) < 16 {
			continue
		}

		if !isPointInRange(pi.fields[i].IdAsBytes(), pi.startTime, pi.endTime, key) {
			continue
		}

		value := it.Value()
		sequenceNumber := key[16:]

		rawTime := key[8:16]
		pi.rawColumnValues[i] = rawColumnValue{time: rawTime, sequence: sequenceNumber, value: value}
	}
	return nil
}

func (pi *PointIterator) Valid() bool {
	return pi.valid
}

func (pi *PointIterator) setError(err error) {
	pi.err = err
	pi.valid = false
}

func (pi *PointIterator) Point() *protocol.Point { return pi.point }

func (pi *PointIterator) Error() error { return pi.err }

func (pi *PointIterator) Close() {
	for _, itr := range pi.itrs {
		itr.Close()
	}
}
