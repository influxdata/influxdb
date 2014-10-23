package datastore

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"sync"
	"time"

	"code.google.com/p/goprotobuf/proto"
	log "code.google.com/p/log4go"
	"github.com/influxdb/influxdb/common"
	"github.com/influxdb/influxdb/datastore/storage"
	"github.com/influxdb/influxdb/engine"
	"github.com/influxdb/influxdb/metastore"
	"github.com/influxdb/influxdb/parser"
	"github.com/influxdb/influxdb/protocol"
)

type Shard struct {
	db             storage.Engine
	closed         bool
	pointBatchSize int
	writeBatchSize int
	metaStore      *metastore.Store
	closeLock      sync.RWMutex
}

func NewShard(db storage.Engine, pointBatchSize, writeBatchSize int, metaStore *metastore.Store) (*Shard, error) {
	return &Shard{
		db:             db,
		pointBatchSize: pointBatchSize,
		writeBatchSize: writeBatchSize,
		metaStore:      metaStore,
	}, nil
}

func (self *Shard) Write(database string, series []*protocol.Series) error {
	self.closeLock.RLock()
	defer self.closeLock.RUnlock()
	if self.closed {
		return fmt.Errorf("Shard is closed")
	}

	wb := make([]storage.Write, 0)

	for _, s := range series {
		if len(s.Points) == 0 {
			return errors.New("Unable to write no data. Series was nil or had no points.")
		}
		if len(s.FieldIds) == 0 {
			return errors.New("Unable to write points without fields")
		}

		count := 0
		for fieldIndex, id := range s.FieldIds {
			for _, point := range s.Points {
				// keyBuffer and dataBuffer have to be recreated since we are
				// batching the writes, otherwise new writes will override the
				// old writes that are still in memory
				dataBuffer := proto.NewBuffer(nil)
				var err error

				sk := newStorageKey(id, point.GetTimestamp(), point.GetSequenceNumber())
				if point.Values[fieldIndex].GetIsNull() {
					wb = append(wb, storage.Write{Key: sk.bytes(), Value: nil})
					goto check
				}

				err = dataBuffer.Marshal(point.Values[fieldIndex])
				if err != nil {
					return err
				}
				wb = append(wb, storage.Write{Key: sk.bytes(), Value: dataBuffer.Bytes()})
			check:
				count++
				if count >= self.writeBatchSize {
					err = self.db.BatchPut(wb)
					if err != nil {
						return err
					}
					count = 0
					wb = make([]storage.Write, 0, self.writeBatchSize)
				}
			}
		}
	}

	return self.db.BatchPut(wb)
}

func (self *Shard) Query(querySpec *parser.QuerySpec, processor engine.Processor) error {
	self.closeLock.RLock()
	defer self.closeLock.RUnlock()
	if self.closed {
		return fmt.Errorf("Shard is closed")
	}
	if querySpec.IsListSeriesQuery() {
		return fmt.Errorf("List series queries should never come to the shard")
	} else if querySpec.IsDeleteFromSeriesQuery() {
		return self.executeDeleteQuery(querySpec, processor)
	}

	seriesAndColumns := querySpec.SelectQuery().GetReferencedColumns()

	if !self.hasReadAccess(querySpec) {
		return errors.New("User does not have access to one or more of the series requested.")
	}

	for series, columns := range seriesAndColumns {
		if regex, ok := series.GetCompiledRegex(); ok {
			seriesNames := self.metaStore.GetSeriesForDatabaseAndRegex(querySpec.Database(), regex)
			for _, name := range seriesNames {
				if !querySpec.HasReadAccess(name) {
					continue
				}
				err := self.executeQueryForSeries(querySpec, name, columns, processor)
				if err != nil {
					return err
				}
			}
		} else {
			err := self.executeQueryForSeries(querySpec, series.Name, columns, processor)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (self *Shard) IsClosed() bool {
	return self.closed
}

func (self *Shard) executeQueryForSeries(querySpec *parser.QuerySpec, seriesName string, columns []string, processor engine.Processor) error {
	fields, err := self.getFieldsForSeries(querySpec.Database(), seriesName, columns)
	if err != nil {
		log.Error("Error looking up fields for %s: %s", seriesName, err)
		return err
	}

	if querySpec.IsSinglePointQuery() {
		log.Debug("Running single query for series %s, fields %v", seriesName, fields)
		return self.executeSinglePointQuery(querySpec, seriesName, fields, processor)
	}

	startTime := querySpec.GetStartTime()
	endTime := querySpec.GetEndTime()

	query := querySpec.SelectQuery()

	aliases := query.GetTableAliases(seriesName)

	fieldNames, iterators := self.getIterators(fields, startTime, endTime, query.Ascending)
	seriesOutgoing := &protocol.Series{Name: protocol.String(seriesName), Fields: fieldNames, Points: make([]*protocol.Point, 0, self.pointBatchSize)}
	pi := NewPointIterator(iterators, fields, querySpec.GetStartTime(), querySpec.GetEndTime(), query.Ascending)
	defer pi.Close()

	for pi.Valid() {
		p := pi.Point()
		seriesOutgoing.Points = append(seriesOutgoing.Points, p)
		if len(seriesOutgoing.Points) >= self.pointBatchSize {
			ok, err := yieldToProcessor(seriesOutgoing, processor, aliases)
			if !ok || err != nil {
				log.Debug("Stopping processing.")
				if err != nil {
					log.Error("Error while processing data: %v", err)
					return err
				}
			}
			seriesOutgoing = &protocol.Series{Name: protocol.String(seriesName), Fields: fieldNames, Points: make([]*protocol.Point, 0, self.pointBatchSize)}
		}

		pi.Next()
	}

	if err := pi.Error(); err != nil {
		return err
	}

	//Yield remaining data
	if ok, err := yieldToProcessor(seriesOutgoing, processor, aliases); !ok || err != nil {
		log.Debug("Stopping processing remaining points...")
		if err != nil {
			log.Error("Error while processing data: %v", err)
			return err
		}
	}

	log.Debug("Finished running query %s", query.GetQueryString())
	return nil
}

func (self *Shard) executeDeleteQuery(querySpec *parser.QuerySpec, processor engine.Processor) error {
	query := querySpec.DeleteQuery()
	series := query.GetFromClause()
	database := querySpec.Database()
	if series.Type != parser.FromClauseArray {
		return fmt.Errorf("Merge and Inner joins can't be used with a delete query: %v", series.Type)
	}

	for _, name := range series.Names {
		var err error
		if regex, ok := name.Name.GetCompiledRegex(); ok {
			err = self.deleteRangeOfRegex(database, regex, query.GetStartTime(), query.GetEndTime())
		} else {
			err = self.deleteRangeOfSeries(database, name.Name.Name, query.GetStartTime(), query.GetEndTime())
		}

		if err != nil {
			return err
		}
	}
	self.db.Compact()
	return nil
}

func (self *Shard) DropFields(fields []*metastore.Field) error {
	self.closeLock.RLock()
	defer self.closeLock.RUnlock()
	if self.closed {
		return fmt.Errorf("Shard is closed")
	}
	return self.deleteRangeOfFields(fields, math.MinInt64, math.MaxInt64)
}

func (self *Shard) deleteRangeOfSeries(database, series string, startTime, endTime time.Time) error {
	fields := self.metaStore.GetFieldsForSeries(database, series)
	st := common.TimeToMicroseconds(startTime)
	et := common.TimeToMicroseconds(endTime)
	return self.deleteRangeOfFields(fields, st, et)
}

func (self *Shard) deleteRangeOfFields(fields []*metastore.Field, st, et int64) error {
	for _, field := range fields {
		sk := newStorageKey(field.Id, st, 0)
		ek := newStorageKey(field.Id, et, maxSeqNumber)

		err := self.db.Del(sk.bytes(), ek.bytes())
		if err != nil {
			return err
		}
	}
	return nil
}

func (self *Shard) deleteRangeOfRegex(database string, regex *regexp.Regexp, startTime, endTime time.Time) error {
	series := self.metaStore.GetSeriesForDatabaseAndRegex(database, regex)
	for _, name := range series {
		err := self.deleteRangeOfSeries(database, name, startTime, endTime)
		if err != nil {
			return err
		}
	}
	return nil
}

func (self *Shard) hasReadAccess(querySpec *parser.QuerySpec) bool {
	for series := range querySpec.SeriesValuesAndColumns() {
		if _, isRegex := series.GetCompiledRegex(); !isRegex {
			if !querySpec.HasReadAccess(series.Name) {
				return false
			}
		}
	}
	return true
}

func (self *Shard) close() {
	self.closeLock.Lock()
	defer self.closeLock.Unlock()
	self.closed = true
	self.db.Close()
	self.db = nil
}

func (self *Shard) executeSinglePointQuery(querySpec *parser.QuerySpec, series string, fields []*metastore.Field, p engine.Processor) error {
	query := querySpec.SelectQuery()
	fieldCount := len(fields)
	fieldNames := make([]string, 0, fieldCount)
	point := &protocol.Point{Values: make([]*protocol.FieldValue, 0, fieldCount)}
	timestamp := common.TimeToMicroseconds(query.GetStartTime())
	sequenceNumber, err := query.GetSinglePointQuerySequenceNumber()
	if err != nil {
		return err
	}

	// set the timestamp and sequence number
	point.SequenceNumber = &sequenceNumber
	point.SetTimestampInMicroseconds(timestamp)

	for _, field := range fields {
		sk := newStorageKey(field.Id, timestamp, sequenceNumber)
		data, err := self.db.Get(sk.bytes())
		if err != nil {
			return err
		}

		if data == nil {
			continue
		}

		fieldValue := &protocol.FieldValue{}
		err = proto.Unmarshal(data, fieldValue)
		if err != nil {
			return err
		}
		fieldNames = append(fieldNames, field.Name)
		point.Values = append(point.Values, fieldValue)
	}

	result := &protocol.Series{Name: &series, Fields: fieldNames, Points: []*protocol.Point{point}}

	if len(result.Points) > 0 {
		_, err := p.Yield(result)
		return err
	}
	return nil
}

func (self *Shard) getIterators(fields []*metastore.Field, start, end time.Time, isAscendingQuery bool) (fieldNames []string, iterators []storage.Iterator) {
	iterators = make([]storage.Iterator, len(fields))
	fieldNames = make([]string, len(fields))

	// start the iterators to go through the series data
	for i, field := range fields {
		fieldNames[i] = field.Name
		iterators[i] = self.db.Iterator()

		t := start
		var seq uint64 = 0
		if !isAscendingQuery {
			t = end
			seq = maxSeqNumber
		}

		tmicro := common.TimeToMicroseconds(t)
		sk := newStorageKey(field.Id, tmicro, seq)
		iterators[i].Seek(sk.bytes())

		if !isAscendingQuery && iterators[i].Valid() {
			iterators[i].Prev()
		}

		if err := iterators[i].Error(); err != nil {
			log.Error("Error while getting iterators: %s", err)
			return nil, nil
		}
	}
	return
}

func (self *Shard) getFieldsForSeries(db, series string, columns []string) ([]*metastore.Field, error) {
	allFields := self.metaStore.GetFieldsForSeries(db, series)
	if len(allFields) == 0 {
		return nil, FieldLookupError{"Couldn't look up columns for series: " + series}
	}
	if len(columns) > 0 && columns[0] == "*" {
		return allFields, nil
	}

	fields := make([]*metastore.Field, len(columns), len(columns))

	for i, name := range columns {
		hasField := false
		for _, f := range allFields {
			if f.Name == name {
				field := f
				hasField = true
				fields[i] = field
				break
			}
		}
		if !hasField {
			return nil, FieldLookupError{"Field " + name + " doesn't exist in series " + series}
		}
	}
	return fields, nil
}

const maxSeqNumber = (1 << 64) - 1

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
	return common.TimeFromMicroseconds(sk.timestamp)
}

// utility functions only used in this file

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

func yieldToProcessor(s *protocol.Series, p engine.Processor, aliases []string) (bool, error) {
	for _, alias := range aliases {
		series := &protocol.Series{
			Name:   proto.String(alias),
			Fields: s.Fields,
			Points: s.Points,
		}
		log4go.Debug("Yielding to %s %s", p.Name(), series)
		if ok, err := p.Yield(series); !ok || err != nil {
			return ok, err
		}
	}
	return true, nil
}
