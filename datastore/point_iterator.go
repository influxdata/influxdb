package datastore

import (
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
	startTime, endTime time.Time
	rawColumnValues    []rawColumnValue
	valid              bool
	err                error
	point              *protocol.Point
	asc                bool
}

// Creates a new point iterator using the given column iterator,
// metadata columns, start and end time as well as the ascending
// flag. The iterator returned is already placed at the first point,
// there's no need to call Next() after the call to NewPointIterator,
// but the user should check Valid() to make sure the iterator is
// pointing at a valid point.
func NewPointIterator(itrs []storage.Iterator, fields []*metastore.Field, startTime, endTime time.Time, asc bool) *PointIterator {
	pi := PointIterator{
		valid:           true,
		err:             nil,
		itrs:            itrs,
		fields:          fields,
		rawColumnValues: make([]rawColumnValue, len(fields)),
		startTime:       startTime,
		endTime:         endTime,
		asc:             asc,
	}

	// seek to the first point
	pi.Next()
	return &pi
}

// public api

// Advance the iterator to the next point
func (pi *PointIterator) Next() {
	valueBuffer := proto.NewBuffer(nil)
	pi.valid = false
	pi.point = &protocol.Point{Values: make([]*protocol.FieldValue, len(pi.fields))}

	err := pi.getIteratorNextValue()
	if err != nil {
		pi.setError(err)
		return
	}

	var next *rawColumnValue

	// choose the highest (or lowest in case of ascending queries) timestamp
	// and sequence number. that will become the timestamp and sequence of
	// the next point.
	for i, value := range pi.rawColumnValues {
		if value.value == nil {
			continue
		}

		if next == nil {
			next = &pi.rawColumnValues[i]
			continue
		}

		if pi.asc {
			if value.before(next) {
				next = &pi.rawColumnValues[i]
			}
			continue
		}

		// the query is descending
		if value.after(next) {
			next = &pi.rawColumnValues[i]
		}
	}

	for i, iterator := range pi.itrs {
		rcv := &pi.rawColumnValues[i]
		log4go.Debug("Column value: %s", rcv)

		// if the value is nil or doesn't match the point's timestamp and sequence number
		// then skip it
		if rcv.value == nil || rcv.time != next.time || rcv.sequence != next.sequence {
			log4go.Debug("rcv = %#v, next = %#v", rcv, next)
			pi.point.Values[i] = &protocol.FieldValue{IsNull: &TRUE}
			continue
		}

		// if we emitted at least one column, then we should keep
		// trying to get more points
		log4go.Debug("Setting is valid to true")
		pi.valid = true

		// advance the iterator to read a new value in the next iteration
		if pi.asc {
			iterator.Next()
		} else {
			iterator.Prev()
		}

		fv := &protocol.FieldValue{}
		valueBuffer.SetBuf(rcv.value)
		err := valueBuffer.Unmarshal(fv)
		if err != nil {
			log4go.Error("Error while running query: %s", err)
			pi.setError(err)
			return
		}
		pi.point.Values[i] = fv
		rcv.value = nil
	}

	// this will only happen if there are no points for the given series
	// and range and this is the first call to Next(). Otherwise we
	// always call Next() on a valid PointIterator so we know we have
	// more points
	if next == nil {
		return
	}

	pi.point.SetTimestampInMicroseconds(next.time)
	pi.point.SequenceNumber = proto.Uint64(next.sequence)
}

// Returns true if the iterator is pointing at a valid
// location. Behavior of Point() is undefined if Valid() is false.
func (pi *PointIterator) Valid() bool {
	return pi.valid
}

// Returns the point that the iterator is pointing to.
func (pi *PointIterator) Point() *protocol.Point { return pi.point }

// Returns an error if the iterator became invalid due to an error as
// opposed to reaching the end time.
func (pi *PointIterator) Error() error { return pi.err }

// Close the iterator and free any resources used by the
// iterator. Behavior of the iterator is undefined if the iterator is
// used after it was closed.
func (pi *PointIterator) Close() {
	for _, itr := range pi.itrs {
		itr.Close()
	}
}

// private api

func (pi *PointIterator) getIteratorNextValue() error {
	for i, it := range pi.itrs {
		if pi.rawColumnValues[i].value != nil {
			log4go.Debug("Value in iterator isn't nil, skipping")
			continue
		}

		if !it.Valid() {
			if err := it.Error(); err != nil {
				return err
			}
			log4go.Debug("Iterator isn't valid, skipping")
			continue
		}

		key := it.Key()
		sk, err := parseKey(key)
		if err != nil {
			panic(err)
		}

		// if we ran out of points for this field go to the next iterator
		if sk.id != pi.fields[i].Id {
			log4go.Debug("Different id reached")
			continue
		}

		// if the point is outside the query start and end time
		if sk.time().Before(pi.startTime) || sk.time().After(pi.endTime) {
			log4go.Debug("Outside time range: %s, %s", sk.time(), pi.startTime)
			continue
		}

		value := it.Value()
		pi.rawColumnValues[i] = rawColumnValue{time: sk.timestamp, sequence: sk.seq, value: value}
		log4go.Debug("Iterator next value: %v", pi.rawColumnValues[i])
	}
	return nil
}

func (pi *PointIterator) setError(err error) {
	pi.err = err
	pi.valid = false
}
