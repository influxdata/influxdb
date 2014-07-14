package datastore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"

	"code.google.com/p/goprotobuf/proto"
	log "code.google.com/p/log4go"
	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/common"
	"github.com/influxdb/influxdb/datastore/storage"
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
				keyBuffer := bytes.NewBuffer(make([]byte, 0, 24))
				dataBuffer := proto.NewBuffer(nil)
				var err error

				binary.Write(keyBuffer, binary.BigEndian, &id)
				timestamp := self.convertTimestampToUint(point.GetTimestampInMicroseconds())
				// pass the uint64 by reference so binary.Write() doesn't create a new buffer
				// see the source code for intDataSize() in binary.go
				binary.Write(keyBuffer, binary.BigEndian, &timestamp)
				binary.Write(keyBuffer, binary.BigEndian, point.SequenceNumber)
				pointKey := keyBuffer.Bytes()

				if point.Values[fieldIndex].GetIsNull() {
					wb = append(wb, storage.Write{Key: pointKey, Value: nil})
					goto check
				}

				err = dataBuffer.Marshal(point.Values[fieldIndex])
				if err != nil {
					return err
				}
				wb = append(wb, storage.Write{Key: pointKey, Value: dataBuffer.Bytes()})
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

func (self *Shard) Query(querySpec *parser.QuerySpec, processor cluster.QueryProcessor) error {
	if querySpec.IsListSeriesQuery() {
		return self.executeListSeriesQuery(querySpec, processor)
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

func (self *Shard) executeQueryForSeries(querySpec *parser.QuerySpec, seriesName string, columns []string, processor cluster.QueryProcessor) error {
	startTimeBytes := self.byteArrayForTime(querySpec.GetStartTime())
	endTimeBytes := self.byteArrayForTime(querySpec.GetEndTime())

	fields, err := self.getFieldsForSeries(querySpec.Database(), seriesName, columns)
	if err != nil {
		log.Error("Error looking up fields for %s: %s", seriesName, err)
		return err
	}

	fieldCount := len(fields)
	rawColumnValues := make([]rawColumnValue, fieldCount, fieldCount)
	query := querySpec.SelectQuery()

	aliases := query.GetTableAliases(seriesName)
	if querySpec.IsSinglePointQuery() {
		series, err := self.fetchSinglePoint(querySpec, seriesName, fields)
		if err != nil {
			log.Error("Error reading a single point: %s", err)
			return err
		}
		if len(series.Points) > 0 {
			processor.YieldPoint(series.Name, series.Fields, series.Points[0])
		}
		return nil
	}

	fieldNames, iterators := self.getIterators(fields, startTimeBytes, endTimeBytes, query.Ascending)
	defer func() {
		for _, it := range iterators {
			it.Close()
		}
	}()

	seriesOutgoing := &protocol.Series{Name: protocol.String(seriesName), Fields: fieldNames, Points: make([]*protocol.Point, 0, self.pointBatchSize)}

	// TODO: clean up, this is super gnarly
	// optimize for the case where we're pulling back only a single column or aggregate
	buffer := bytes.NewBuffer(nil)
	valueBuffer := proto.NewBuffer(nil)
	for {
		isValid := false
		point := &protocol.Point{Values: make([]*protocol.FieldValue, fieldCount, fieldCount)}

		for i, it := range iterators {
			if rawColumnValues[i].value != nil || !it.Valid() {
				if err := it.Error(); err != nil {
					return err
				}
				continue
			}

			key := it.Key()
			if len(key) < 16 {
				continue
			}

			if !isPointInRange(fields[i].IdAsBytes(), startTimeBytes, endTimeBytes, key) {
				continue
			}

			value := it.Value()
			sequenceNumber := key[16:]

			rawTime := key[8:16]
			rawColumnValues[i] = rawColumnValue{time: rawTime, sequence: sequenceNumber, value: value}
		}

		var pointTimeRaw []byte
		var pointSequenceRaw []byte
		// choose the highest (or lowest in case of ascending queries) timestamp
		// and sequence number. that will become the timestamp and sequence of
		// the next point.
		for _, value := range rawColumnValues {
			if value.value == nil {
				continue
			}

			pointTimeRaw, pointSequenceRaw = value.updatePointTimeAndSequence(pointTimeRaw,
				pointSequenceRaw, query.Ascending)
		}

		for i, iterator := range iterators {
			// if the value is nil or doesn't match the point's timestamp and sequence number
			// then skip it
			if rawColumnValues[i].value == nil ||
				!bytes.Equal(rawColumnValues[i].time, pointTimeRaw) ||
				!bytes.Equal(rawColumnValues[i].sequence, pointSequenceRaw) {

				point.Values[i] = &protocol.FieldValue{IsNull: &TRUE}
				continue
			}

			// if we emitted at lease one column, then we should keep
			// trying to get more points
			isValid = true

			// advance the iterator to read a new value in the next iteration
			if query.Ascending {
				iterator.Next()
			} else {
				iterator.Prev()
			}

			fv := &protocol.FieldValue{}
			valueBuffer.SetBuf(rawColumnValues[i].value)
			err := valueBuffer.Unmarshal(fv)
			if err != nil {
				log.Error("Error while running query: %s", err)
				return err
			}
			point.Values[i] = fv
			rawColumnValues[i].value = nil
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

		time := self.convertUintTimestampToInt64(&t)
		point.SetTimestampInMicroseconds(time)
		point.SequenceNumber = &sequence

		// stop the loop if we ran out of points
		if !isValid {
			break
		}

		shouldContinue := true

		seriesOutgoing.Points = append(seriesOutgoing.Points, point)

		if len(seriesOutgoing.Points) >= self.pointBatchSize {
			for _, alias := range aliases {
				series := &protocol.Series{
					Name:   proto.String(alias),
					Fields: fieldNames,
					Points: seriesOutgoing.Points,
				}
				if !processor.YieldSeries(series) {
					log.Info("Stopping processing")
					shouldContinue = false
				}
			}
			seriesOutgoing = &protocol.Series{Name: protocol.String(seriesName), Fields: fieldNames, Points: make([]*protocol.Point, 0, self.pointBatchSize)}
		}

		if !shouldContinue {
			break
		}
	}

	//Yield remaining data
	for _, alias := range aliases {
		log.Debug("Final Flush %s", alias)
		series := &protocol.Series{Name: protocol.String(alias), Fields: seriesOutgoing.Fields, Points: seriesOutgoing.Points}
		if !processor.YieldSeries(series) {
			log.Debug("Cancelled...")
		}
	}

	log.Debug("Finished running query %s", query.GetQueryString())
	return nil
}

func (self *Shard) yieldSeriesNamesForDb(db string, yield func(string) bool) error {
	dbNameStart := len(DATABASE_SERIES_INDEX_PREFIX)
	pred := func(key []byte) bool {
		return len(key) >= dbNameStart && bytes.Equal(key[:dbNameStart], DATABASE_SERIES_INDEX_PREFIX)
	}

	firstKey := append(DATABASE_SERIES_INDEX_PREFIX, []byte(db+"~")...)
	itr := self.db.Iterator()
	defer itr.Close()

	for itr.Seek(firstKey); itr.Valid(); itr.Next() {
		key := itr.Key()
		if !pred(key) {
			break
		}
		dbSeries := string(key[dbNameStart:])
		parts := strings.Split(dbSeries, "~")
		if len(parts) > 1 {
			if parts[0] != db {
				break
			}
			name := parts[1]
			shouldContinue := yield(name)
			if !shouldContinue {
				return nil
			}
		}
	}
	if err := itr.Error(); err != nil {
		return err
	}
	return nil
}

func (self *Shard) executeListSeriesQuery(querySpec *parser.QuerySpec, processor cluster.QueryProcessor) error {
	return self.yieldSeriesNamesForDb(querySpec.Database(), func(_name string) bool {
		name := _name
		return processor.YieldPoint(&name, nil, nil)
	})
}

func (self *Shard) executeDeleteQuery(querySpec *parser.QuerySpec, processor cluster.QueryProcessor) error {
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
	startTimeBytes := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	endTimeBytes := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	return self.deleteRangeOfFields(fields, startTimeBytes, endTimeBytes)
}

func (self *Shard) byteArrayForTimeInt(time int64) []byte {
	timeBuffer := bytes.NewBuffer(make([]byte, 0, 8))
	binary.Write(timeBuffer, binary.BigEndian, self.convertTimestampToUint(&time))
	bytes := timeBuffer.Bytes()
	return bytes
}

func (self *Shard) byteArraysForStartAndEndTimes(startTime, endTime int64) ([]byte, []byte) {
	return self.byteArrayForTimeInt(startTime), self.byteArrayForTimeInt(endTime)
}

func (self *Shard) deleteRangeOfSeriesCommon(database, series string, startTimeBytes, endTimeBytes []byte) error {
	fields := self.metaStore.GetFieldsForSeries(database, series)
	return self.deleteRangeOfFields(fields, startTimeBytes, endTimeBytes)
}

func (self *Shard) deleteRangeOfFields(fields []*metastore.Field, startTimeBytes, endTimeBytes []byte) error {
	startKey := bytes.NewBuffer(nil)
	endKey := bytes.NewBuffer(nil)
	for _, field := range fields {
		idBytes := field.IdAsBytes()
		startKey.Reset()
		startKey.Write(idBytes)
		startKey.Write(startTimeBytes)
		startKey.Write([]byte{0, 0, 0, 0, 0, 0, 0, 0})
		endKey.Reset()
		endKey.Write(idBytes)
		endKey.Write(endTimeBytes)
		endKey.Write([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})

		err := self.db.Del(startKey.Bytes(), endKey.Bytes())
		if err != nil {
			return err
		}
	}
	return nil
}

// func (self *Shard) compact() {
// 	log.Info("Compacting shard")
// 	self.db.CompactRange(levigo.Range{})
// 	log.Info("Shard compaction is done")
// }

func (self *Shard) deleteRangeOfSeries(database, series string, startTime, endTime time.Time) error {
	startTimeBytes, endTimeBytes := self.byteArraysForStartAndEndTimes(common.TimeToMicroseconds(startTime), common.TimeToMicroseconds(endTime))
	return self.deleteRangeOfSeriesCommon(database, series, startTimeBytes, endTimeBytes)
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

func (self *Shard) byteArrayForTime(t time.Time) []byte {
	timeBuffer := bytes.NewBuffer(make([]byte, 0, 8))
	timeMicro := common.TimeToMicroseconds(t)
	binary.Write(timeBuffer, binary.BigEndian, self.convertTimestampToUint(&timeMicro))
	return timeBuffer.Bytes()
}

func (self *Shard) close() {
	self.closed = true
	self.db.Close()
	self.db = nil
}

func (self *Shard) convertTimestampToUint(t *int64) uint64 {
	if *t < 0 {
		return uint64(math.MaxInt64 + *t + 1)
	}
	return uint64(*t) + uint64(math.MaxInt64) + uint64(1)
}

func (self *Shard) fetchSinglePoint(querySpec *parser.QuerySpec, series string, fields []*metastore.Field) (*protocol.Series, error) {
	query := querySpec.SelectQuery()
	fieldCount := len(fields)
	fieldNames := make([]string, 0, fieldCount)
	point := &protocol.Point{Values: make([]*protocol.FieldValue, 0, fieldCount)}
	timestamp := common.TimeToMicroseconds(query.GetStartTime())
	sequenceNumber, err := query.GetSinglePointQuerySequenceNumber()
	if err != nil {
		return nil, err
	}

	timeAndSequenceBuffer := bytes.NewBuffer(make([]byte, 0, 16))
	binary.Write(timeAndSequenceBuffer, binary.BigEndian, self.convertTimestampToUint(&timestamp))
	binary.Write(timeAndSequenceBuffer, binary.BigEndian, sequenceNumber)
	sequenceNumber_uint64 := uint64(sequenceNumber)
	point.SequenceNumber = &sequenceNumber_uint64
	point.SetTimestampInMicroseconds(timestamp)

	timeAndSequenceBytes := timeAndSequenceBuffer.Bytes()
	for _, field := range fields {
		pointKeyBuff := bytes.NewBuffer(make([]byte, 0, 24))
		pointKeyBuff.Write(field.IdAsBytes())
		pointKeyBuff.Write(timeAndSequenceBytes)

		if data, err := self.db.Get(pointKeyBuff.Bytes()); err != nil {
			return nil, err
		} else {
			fieldValue := &protocol.FieldValue{}
			err := proto.Unmarshal(data, fieldValue)
			if err != nil {
				return nil, err
			}
			if data != nil {
				fieldNames = append(fieldNames, field.Name)
				point.Values = append(point.Values, fieldValue)
			}
		}
	}

	result := &protocol.Series{Name: &series, Fields: fieldNames, Points: []*protocol.Point{point}}

	return result, nil
}

func (self *Shard) getIterators(fields []*metastore.Field, start, end []byte, isAscendingQuery bool) (fieldNames []string, iterators []storage.Iterator) {
	iterators = make([]storage.Iterator, len(fields))
	fieldNames = make([]string, len(fields))

	// start the iterators to go through the series data
	for i, field := range fields {
		idBytes := field.IdAsBytes()
		fieldNames[i] = field.Name
		iterators[i] = self.db.Iterator()

		if isAscendingQuery {
			iterators[i].Seek(append(idBytes, start...))
		} else {
			iterators[i].Seek(append(append(idBytes, end...), MAX_SEQUENCE...))
			if iterators[i].Valid() {
				iterators[i].Prev()
			}
		}

		if err := iterators[i].Error(); err != nil {
			log.Error("Error while getting iterators: %s", err)
			return nil, nil
		}
	}
	return
}

func (self *Shard) convertUintTimestampToInt64(t *uint64) int64 {
	if *t > uint64(math.MaxInt64) {
		return int64(*t-math.MaxInt64) - int64(1)
	}
	return int64(*t) - math.MaxInt64 - int64(1)
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

/* DEPRECATED methods do not use*/

// TODO: remove this on version 0.9 after people have had a chance to do migrations
func (self *Shard) getSeriesForDbAndRegexDEPRECATED(database string, regex *regexp.Regexp) []string {
	names := []string{}
	allSeries := self.metaStore.GetSeriesForDatabase(database)
	for _, name := range allSeries {
		if regex.MatchString(name) {
			names = append(names, name)
		}
	}
	return names
}

// TODO: remove this on version 0.9 after people have had a chance to do migrations
func (self *Shard) getSeriesForDatabaseDEPRECATED(database string) (series []string) {
	err := self.yieldSeriesNamesForDb(database, func(name string) bool {
		series = append(series, name)
		return true
	})
	if err != nil {
		log.Error("Cannot get series names for db %s: %s", database, err)
		return nil
	}
	return series
}

// TODO: remove this function. I'm keeping it around for the moment since it'll probably have to be
//       used in the DB upgrate/migration that moves metadata from the shard to Raft
func (self *Shard) getFieldsForSeriesDEPRECATED(db, series string, columns []string) ([]*metastore.Field, error) {
	isCountQuery := false
	if len(columns) > 0 && columns[0] == "*" {
		columns = self.getColumnNamesForSeriesDEPRECATED(db, series)
	} else if len(columns) == 0 {
		isCountQuery = true
		columns = self.getColumnNamesForSeriesDEPRECATED(db, series)
	}
	if len(columns) == 0 {
		return nil, FieldLookupError{"Couldn't look up columns for series: " + series}
	}

	fields := make([]*metastore.Field, len(columns), len(columns))

	for i, name := range columns {
		id, errId := self.getIdForDbSeriesColumnDEPRECATED(&db, &series, &name)
		if errId != nil {
			return nil, errId
		}
		if id == nil {
			return nil, FieldLookupError{"Field " + name + " doesn't exist in series " + series}
		}
		idInt, err := binary.ReadUvarint(bytes.NewBuffer(id))
		if err != nil {
			return nil, err
		}
		fields[i] = &metastore.Field{Name: name, Id: idInt}
	}

	// if it's a count query we just want the column that will be the most efficient to
	// scan through. So find that and return it.
	if isCountQuery {
		bestField := fields[0]
		return []*metastore.Field{bestField}, nil
	}
	return fields, nil
}

// TODO: remove this function. I'm keeping it around for the moment since it'll probably have to be
//       used in the DB upgrate/migration that moves metadata from the shard to Raft
func (self *Shard) getColumnNamesForSeriesDEPRECATED(db, series string) []string {
	dbNameStart := len(SERIES_COLUMN_INDEX_PREFIX)
	seekKey := append(SERIES_COLUMN_INDEX_PREFIX, []byte(db+"~"+series+"~")...)
	pred := func(key []byte) bool {
		return len(key) >= dbNameStart && bytes.Equal(key[:dbNameStart], SERIES_COLUMN_INDEX_PREFIX)
	}
	it := self.db.Iterator()
	defer it.Close()

	names := make([]string, 0)
	for it.Seek(seekKey); it.Valid(); it.Next() {
		key := it.Key()
		if !pred(key) {
			break
		}
		dbSeriesColumn := string(key[dbNameStart:])
		parts := strings.Split(dbSeriesColumn, "~")
		if len(parts) > 2 {
			if parts[0] != db || parts[1] != series {
				break
			}
			names = append(names, parts[2])
		}
	}
	if err := it.Error(); err != nil {
		log.Error("Error while getting columns for series %s: %s", series, err)
		return nil
	}
	return names
}

// TODO: remove this after a version that doesn't support migration from old non-raft metastore
func (self *Shard) getIdForDbSeriesColumnDEPRECATED(db, series, column *string) (ret []byte, err error) {
	s := fmt.Sprintf("%s~%s~%s", *db, *series, *column)
	b := []byte(s)
	key := append(SERIES_COLUMN_INDEX_PREFIX, b...)
	if ret, err = self.db.Get(key); err != nil {
		return nil, err
	}
	return ret, nil
}
