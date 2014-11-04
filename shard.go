package influxdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"code.google.com/p/goprotobuf/proto"
	"code.google.com/p/log4go"
	"github.com/boltdb/bolt"
	"github.com/influxdb/influxdb/engine"
	"github.com/influxdb/influxdb/parser"
	"github.com/influxdb/influxdb/protocol"
)

// shard represents the physical storage for a given time range.
type shard struct {
	id    uint32
	tmin  int64
	tmax  int64
	hosts []uint32 // server ids
	store *bolt.DB
}

// write writes series data to a shard.
func (s *shard) write(series *protocol.Series) error {
	return s.store.Update(func(tx *bolt.Tx) error {
		// TODO: Write data.
		return nil
	})
}

func (s *shard) deleteSeries(name string) error {
	panic("not yet implemented") // TODO
}

func (s *shard) query(q *parser.QuerySpec, name string, fields Fields, processor engine.Processor) error {
	// TODO? Single point query.
	/*
		if q.IsSinglePointQuery() {
			log4go.Debug("Running single query for series %s, fields %v", name, fields)
			return s.executeSinglePointQuery(q, name, fields, processor)
		}
	*/

	fnames := fields.Names()
	aliases := q.SelectQuery().GetTableAliases(name)

	// Create a new iterator.
	i, _ := s.iterator(fields) // TODO: check error
	i.startTime = q.GetStartTime()
	i.endTime = q.GetEndTime()
	i.ascending = q.SelectQuery().Ascending
	defer func() { _ = i.close() }()

	// Iterate over each point and yield to the processor for each alias.
	for p := i.first(); p != nil; p = i.next() {
		for _, alias := range aliases {
			series := &protocol.Series{
				Name:   proto.String(alias),
				Fields: fnames,
				Points: []*protocol.Point{p},
			}

			log4go.Debug("Yielding to %s %s", processor.Name(), series)
			if ok, err := processor.Yield(series); err != nil {
				log4go.Error("Error while processing data: %v", err)
				return err
			} else if !ok {
				log4go.Debug("Stopping processing.")
				// NOTE: This doesn't stop processing.
			}
		}
	}

	log4go.Debug("Finished running query %s", q.GetQueryString())
	return nil
}

// iterator returns a new iterator for a set of fields.
func (s *shard) iterator(fields []*Field) (*iterator, error) {
	// Open a read-only transaction.
	// This transaction must be closed separately by the iterator.
	tx, err := s.store.Begin(false)
	if err != nil {
		return nil, err
	}

	// Initialize cursor.
	i := &iterator{
		tx:     tx,
		fields: fields,
		values: make([]rawValue, len(fields)),
	}

	// Open a cursor for each field.
	//for _, f := range fields {
	// TODO: Open field cursor.
	//}

	return i, nil
}

// storageKey is the key that we use to store values in our key/value
// store engine. The key contains the field id, timestamp and sequence
// number of the value being stored.
type storageKey struct {
	bytesBuf  []byte
	id        uint64
	timestamp int64
	seq       uint64
}

// Create a new storageKey.
//    timestamp: the timestamp in microseconds. timestamp can be negative.
func newStorageKey(id uint64, timestamp int64, seq uint64) storageKey {
	return storageKey{
		bytesBuf:  nil,
		id:        id,
		timestamp: timestamp,
		seq:       seq,
	}
}

// Parse the given byte slice into a storageKey
func parseKey(b []byte) (storageKey, error) {
	if len(b) != 8*3 {
		return storageKey{}, fmt.Errorf("Expected %d fields, found %d", 8*3, len(b))
	}

	sk := storageKey{}
	buf := bytes.NewBuffer(b)
	binary.Read(buf, binary.BigEndian, &sk.id)
	var t uint64
	binary.Read(buf, binary.BigEndian, &t)
	sk.timestamp = convertUintTimestampToInt64(t)
	binary.Read(buf, binary.BigEndian, &sk.seq)
	sk.bytesBuf = b
	return sk, nil
}

// mustParseKey parses a storage key and panics if cannot be parsed.
func mustParseKey(b []byte) storageKey {
	sk, err := parseKey(b)
	if err != nil {
		panic(err)
	}
	return sk
}

// Return a byte representation of the storage key. If the given byte
// representation was to be lexicographic sorted, then b1 < b2 iff
// id1 < id2 (b1 is a byte representation of a storageKey with a smaller
// id) or id1 == id2 and t1 < t2, or id1 == id2 and t1 == t2 and
// seq1 < seq2. This means that the byte representation has the same
// sort properties as the tuple (id, time, sequence)
func (sk storageKey) bytes() []byte {
	if sk.bytesBuf != nil {
		return sk.bytesBuf
	}

	buf := bytes.NewBuffer(nil)
	binary.Write(buf, binary.BigEndian, sk.id)
	t := convertTimestampToUint(sk.timestamp)
	binary.Write(buf, binary.BigEndian, t)
	binary.Write(buf, binary.BigEndian, sk.seq)
	sk.bytesBuf = buf.Bytes()
	return sk.bytesBuf
}

func (sk storageKey) time() time.Time {
	return time.Unix(0, sk.timestamp*int64(time.Microsecond))
}

func convertTimestampToUint(t int64) uint64 {
	if t < 0 {
		return uint64(math.MaxInt64 + t + 1)
	}
	return uint64(t) + uint64(math.MaxInt64) + uint64(1)
}

func convertUintTimestampToInt64(t uint64) int64 {
	if t > uint64(math.MaxInt64) {
		return int64(t-math.MaxInt64) - int64(1)
	}
	return int64(t) - math.MaxInt64 - int64(1)
}

// iterator takes a slice of iterators and their corresponding
// fields and turn it into a point iterator, i.e. an iterator that
// yields whole points instead of column values.
type iterator struct {
	tx      *bolt.Tx
	cursors []*bolt.Cursor

	fields []*Field
	values []rawValue

	startTime time.Time
	endTime   time.Time
	ascending bool

	valid bool
	err   error

	point *protocol.Point
}

// close closes the read transaction and clears the cursors.
func (i *iterator) close() (err error) {
	err = i.tx.Rollback()
	i.tx = nil
	i.cursors = nil
	return
}

// first moves the iterator to the first point.
func (i *iterator) first() *protocol.Point {
	panic("not yet implemented") // TODO
}

// next moves the cursor to the next point.
func (i *iterator) next() *protocol.Point {
	// Create a new point.
	buf := proto.NewBuffer(nil)
	i.valid = false
	point := &protocol.Point{Values: make([]*protocol.FieldValue, len(i.fields))}

	// Move the cursors to the next value.
	i.nextValue()

	// choose the highest (or lowest in case of ascending queries) timestamp
	// and sequence number. that will become the timestamp and sequence of
	// the next point.
	var next *rawValue
	for j, value := range i.values {
		// Ignore nil values.
		if value.value == nil {
			continue
		}

		// Initialize next value if not set.
		// Otherwise override next if value is before (ASC) or after (DESC).
		if next == nil {
			next = &i.values[j]
		} else if i.ascending && value.before(next) {
			next = &i.values[j]
		} else if !i.ascending && value.after(next) {
			next = &i.values[j]
		}
	}

	// Set values to point that match the timestamp & sequence number.
	for j, c := range i.cursors {
		value := &i.values[j]
		log4go.Debug("Column value: %s", value)

		// Skip value if it doesn't match point's timestamp and seq number.
		if value.value == nil || value.time != next.time || value.sequence != next.sequence {
			trueValue := true
			point.Values[j] = &protocol.FieldValue{IsNull: &trueValue}
			continue
		}

		// Iterator is valid if at least one column is set.
		log4go.Debug("Setting is valid to true")
		i.valid = true

		// Advance the iterator to read a new value in the next iteration
		if i.ascending {
			c.Next()
		} else {
			c.Prev()
		}

		// Marshal value from protobufs into point.
		fv := &protocol.FieldValue{}
		buf.SetBuf(value.value)
		if err := buf.Unmarshal(fv); err != nil {
			log4go.Error("Error while running query: %s", err)
			i.err, i.valid = err, false
			return nil
		}
		point.Values[j] = fv
		value.value = nil
	}

	// This will only happen if there are no points for the given series
	// and range and this is the first call to Next(). Otherwise we
	// always call Next() on a valid iterator so we know we have
	// more points
	if next == nil {
		return nil
	}

	// Set timestamp and sequence number on point.
	point.SetTimestampInMicroseconds(next.time)
	point.SequenceNumber = proto.Uint64(next.sequence)

	return point
}

// nextValue moves cursors forward that don't have a value.
func (i *iterator) nextValue() {
	for j, c := range i.cursors {
		// Ignore cursors which already have a value.
		if i.values[j].value != nil {
			continue
		}

		// Read next key/value.
		k, v := c.Next()

		// Move to the next iterator if different field reached.
		// Move to the next iterator if outside of time range.
		sk := mustParseKey(k)
		if sk.id != i.fields[j].ID {
			log4go.Debug("different id reached")
			continue
		} else if sk.time().Before(i.startTime) || sk.time().After(i.endTime) {
			log4go.Debug("Outside time range: %s, %s", sk.time(), i.startTime)
			continue
		}

		// Set the value for the field.
		i.values[j] = rawValue{time: sk.timestamp, sequence: sk.seq, value: v}
		log4go.Debug("Iterator next value: %v", i.values[j])
	}
}

// rawValue represents the value for a field at a given time and sequence id.
type rawValue struct {
	time     int64
	sequence uint64
	value    []byte
}

// before returns true if the value is before another value.
func (v rawValue) before(other *rawValue) bool {
	return (v.time < other.time) || (v.time == other.time && v.sequence < other.sequence)
}

// after returns true if the value is after another value.
func (v rawValue) after(other *rawValue) bool {
	return (v.time > other.time) || (v.time == other.time && v.sequence > other.sequence)
}

func (v rawValue) String() string {
	return fmt.Sprintf("[time: %d, sequence: %d, value: %v]", v.time, v.sequence, v.value)
}
