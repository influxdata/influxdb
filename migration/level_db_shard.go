package migration

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/common"
	. "github.com/influxdb/influxdb/datastore"
	"github.com/influxdb/influxdb/parser"
	"github.com/influxdb/influxdb/protocol"

	"code.google.com/p/goprotobuf/proto"
	log "code.google.com/p/log4go"
	"github.com/jmhodges/levigo"
)

/*
  This is deprecated. It's only sitting around because people will need it when
  migrating their data from 0.7 to 0.8. We'll remove it later.
*/

type LevelDbShard struct {
	db             *levigo.DB
	readOptions    *levigo.ReadOptions
	writeOptions   *levigo.WriteOptions
	lastIdUsed     uint64
	columnIdMutex  sync.Mutex
	closed         bool
	pointBatchSize int
	writeBatchSize int
}

type Field struct {
	Id   []byte
	Name string
}

type rawColumnValue struct {
	time     []byte
	sequence []byte
	value    []byte
}

// // returns true if the point has the correct field id and is
// // in the given time range
func isPointInRange(fieldId, startTime, endTime, point []byte) bool {
	id := point[:8]
	time := point[8:16]
	return bytes.Equal(id, fieldId) && bytes.Compare(time, startTime) > -1 && bytes.Compare(time, endTime) < 1
}

// depending on the query order (whether it's ascending or not) returns
// the min (or max in case of descending query) of the current
// [timestamp,sequence] and the self's [timestamp,sequence]
//
// This is used to determine what the next point's timestamp
// and sequence number should be.
func (self *rawColumnValue) updatePointTimeAndSequence(currentTimeRaw, currentSequenceRaw []byte, isAscendingQuery bool) ([]byte, []byte) {
	if currentTimeRaw == nil {
		return self.time, self.sequence
	}

	compareValue := 1
	if isAscendingQuery {
		compareValue = -1
	}

	timeCompare := bytes.Compare(self.time, currentTimeRaw)
	if timeCompare == compareValue {
		return self.time, self.sequence
	}

	if timeCompare != 0 {
		return currentTimeRaw, currentSequenceRaw
	}

	if bytes.Compare(self.sequence, currentSequenceRaw) == compareValue {
		return currentTimeRaw, self.sequence
	}

	return currentTimeRaw, currentSequenceRaw
}

func NewLevelDbShard(db *levigo.DB, pointBatchSize, writeBatchSize int) (*LevelDbShard, error) {
	ro := levigo.NewReadOptions()
	lastIdBytes, err2 := db.Get(ro, NEXT_ID_KEY)
	if err2 != nil {
		return nil, err2
	}

	lastId := uint64(0)
	if lastIdBytes != nil {
		lastId, err2 = binary.ReadUvarint(bytes.NewBuffer(lastIdBytes))
		if err2 != nil {
			return nil, err2
		}
	}

	return &LevelDbShard{
		db:             db,
		writeOptions:   levigo.NewWriteOptions(),
		readOptions:    ro,
		lastIdUsed:     lastId,
		pointBatchSize: pointBatchSize,
		writeBatchSize: writeBatchSize,
	}, nil
}

func (self *LevelDbShard) Write(database string, series []*protocol.Series) error {
	wb := levigo.NewWriteBatch()
	defer wb.Close()

	for _, s := range series {
		if len(s.Points) == 0 {
			return errors.New("Unable to write no data. Series was nil or had no points.")
		}

		count := 0
		for fieldIndex, field := range s.Fields {
			temp := field
			id, err := self.createIdForDbSeriesColumn(&database, s.Name, &temp)
			if err != nil {
				return err
			}
			keyBuffer := bytes.NewBuffer(make([]byte, 0, 24))
			dataBuffer := proto.NewBuffer(nil)
			for _, point := range s.Points {
				keyBuffer.Reset()
				dataBuffer.Reset()

				keyBuffer.Write(id)
				timestamp := self.convertTimestampToUint(point.GetTimestampInMicroseconds())
				// pass the uint64 by reference so binary.Write() doesn't create a new buffer
				// see the source code for intDataSize() in binary.go
				binary.Write(keyBuffer, binary.BigEndian, &timestamp)
				binary.Write(keyBuffer, binary.BigEndian, point.SequenceNumber)
				pointKey := keyBuffer.Bytes()

				if point.Values[fieldIndex].GetIsNull() {
					wb.Delete(pointKey)
					goto check
				}

				err = dataBuffer.Marshal(point.Values[fieldIndex])
				if err != nil {
					return err
				}
				wb.Put(pointKey, dataBuffer.Bytes())
			check:
				count++
				if count >= self.writeBatchSize {
					err = self.db.Write(self.writeOptions, wb)
					if err != nil {
						return err
					}
					count = 0
					wb.Clear()
				}
			}
		}
	}

	return self.db.Write(self.writeOptions, wb)
}

func (self *LevelDbShard) Query(querySpec *parser.QuerySpec, processor cluster.QueryProcessor) error {
	if querySpec.IsListSeriesQuery() {
		return self.executeListSeriesQuery(querySpec, processor)
	} else if querySpec.IsDeleteFromSeriesQuery() {
		return self.executeDeleteQuery(querySpec, processor)
	} else if querySpec.IsDropSeriesQuery() {
		return self.executeDropSeriesQuery(querySpec, processor)
	}

	seriesAndColumns := querySpec.SelectQuery().GetReferencedColumns()

	if !self.hasReadAccess(querySpec) {
		return errors.New("User does not have access to one or more of the series requested.")
	}

	for series, columns := range seriesAndColumns {
		if regex, ok := series.GetCompiledRegex(); ok {
			seriesNames := self.getSeriesForDbAndRegex(querySpec.Database(), regex)
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

func (self *LevelDbShard) DropDatabase(database string) error {
	seriesNames := self.GetSeriesForDatabase(database)
	for _, name := range seriesNames {
		if err := self.dropSeries(database, name); err != nil {
			log.Error("DropDatabase: ", err)
			return err
		}
	}
	self.compact()
	return nil
}

func (self *LevelDbShard) IsClosed() bool {
	return self.closed
}

func (self *LevelDbShard) executeQueryForSeries(querySpec *parser.QuerySpec, seriesName string, columns []string, processor cluster.QueryProcessor) error {
	startTimeBytes := self.byteArrayForTime(querySpec.GetStartTime())
	endTimeBytes := self.byteArrayForTime(querySpec.GetEndTime())

	fields, err := self.getFieldsForSeries(querySpec.Database(), seriesName, columns)
	if err != nil {
		// because a db is distributed across the cluster, it's possible we don't have the series indexed here. ignore
		switch err := err.(type) {
		case FieldLookupError:
			log.Debug("Cannot find fields %v", columns)
			return nil
		default:
			log.Error("Error looking up fields for %s: %s", seriesName, err)
			return fmt.Errorf("Error looking up fields for %s: %s", seriesName, err)
		}
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
				continue
			}

			key := it.Key()
			if len(key) < 16 {
				continue
			}

			if !isPointInRange(fields[i].Id, startTimeBytes, endTimeBytes, key) {
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

func (self *LevelDbShard) executeListSeriesQuery(querySpec *parser.QuerySpec, processor cluster.QueryProcessor) error {
	it := self.db.NewIterator(self.readOptions)
	defer it.Close()

	database := querySpec.Database()
	seekKey := append(DATABASE_SERIES_INDEX_PREFIX, []byte(querySpec.Database()+"~")...)
	it.Seek(seekKey)
	dbNameStart := len(DATABASE_SERIES_INDEX_PREFIX)
	for it = it; it.Valid(); it.Next() {
		key := it.Key()
		if len(key) < dbNameStart || !bytes.Equal(key[:dbNameStart], DATABASE_SERIES_INDEX_PREFIX) {
			break
		}
		dbSeries := string(key[dbNameStart:])
		parts := strings.Split(dbSeries, "~")
		if len(parts) > 1 {
			if parts[0] != database {
				break
			}
			name := parts[1]
			shouldContinue := processor.YieldPoint(&name, nil, nil)
			if !shouldContinue {
				return nil
			}
		}
	}
	return nil
}

func (self *LevelDbShard) executeDeleteQuery(querySpec *parser.QuerySpec, processor cluster.QueryProcessor) error {
	query := querySpec.DeleteQuery()
	series := query.GetFromClause()
	database := querySpec.Database()
	if series.Type != parser.FromClauseArray {
		return fmt.Errorf("Merge and Inner joins can't be used with a delete query", series.Type)
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
	self.compact()
	return nil
}

func (self *LevelDbShard) executeDropSeriesQuery(querySpec *parser.QuerySpec, processor cluster.QueryProcessor) error {
	database := querySpec.Database()
	series := querySpec.Query().DropSeriesQuery.GetTableName()
	err := self.dropSeries(database, series)
	if err != nil {
		return err
	}
	self.compact()
	return nil
}

func (self *LevelDbShard) dropSeries(database, series string) error {
	startTimeBytes := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	endTimeBytes := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}

	wb := levigo.NewWriteBatch()
	defer wb.Close()

	if err := self.deleteRangeOfSeriesCommon(database, series, startTimeBytes, endTimeBytes); err != nil {
		return err
	}

	for _, name := range self.getColumnNamesForSeries(database, series) {
		indexKey := append(SERIES_COLUMN_INDEX_PREFIX, []byte(database+"~"+series+"~"+name)...)
		wb.Delete(indexKey)
	}

	wb.Delete(append(DATABASE_SERIES_INDEX_PREFIX, []byte(database+"~"+series)...))

	// remove the column indeces for this time series
	return self.db.Write(self.writeOptions, wb)
}

func (self *LevelDbShard) byteArrayForTimeInt(time int64) []byte {
	timeBuffer := bytes.NewBuffer(make([]byte, 0, 8))
	binary.Write(timeBuffer, binary.BigEndian, self.convertTimestampToUint(&time))
	bytes := timeBuffer.Bytes()
	return bytes
}

func (self *LevelDbShard) byteArraysForStartAndEndTimes(startTime, endTime int64) ([]byte, []byte) {
	return self.byteArrayForTimeInt(startTime), self.byteArrayForTimeInt(endTime)
}

func (self *LevelDbShard) deleteRangeOfSeriesCommon(database, series string, startTimeBytes, endTimeBytes []byte) error {
	columns := self.getColumnNamesForSeries(database, series)
	fields, err := self.getFieldsForSeries(database, series, columns)
	if err != nil {
		// because a db is distributed across the cluster, it's possible we don't have the series indexed here. ignore
		switch err := err.(type) {
		case FieldLookupError:
			return nil
		default:
			return err
		}
	}
	ro := levigo.NewReadOptions()
	defer ro.Close()
	ro.SetFillCache(false)
	wb := levigo.NewWriteBatch()
	count := 0
	defer wb.Close()
	for _, field := range fields {
		it := self.db.NewIterator(ro)
		defer it.Close()

		startKey := append(field.Id, startTimeBytes...)
		it.Seek(startKey)
		if it.Valid() {
			if !bytes.Equal(it.Key()[:8], field.Id) {
				it.Next()
				if it.Valid() {
					startKey = it.Key()
				}
			}
		}
		for it = it; it.Valid(); it.Next() {
			k := it.Key()
			if len(k) < 16 || !bytes.Equal(k[:8], field.Id) || bytes.Compare(k[8:16], endTimeBytes) == 1 {
				break
			}
			wb.Delete(k)
			count++
			if count >= self.writeBatchSize {
				err = self.db.Write(self.writeOptions, wb)
				if err != nil {
					return err
				}
				count = 0
				wb.Clear()
			}
		}
	}
	return self.db.Write(self.writeOptions, wb)
}

func (self *LevelDbShard) compact() {
	log.Info("Compacting shard")
	self.db.CompactRange(levigo.Range{})
	log.Info("Shard compaction is done")
}

func (self *LevelDbShard) deleteRangeOfSeries(database, series string, startTime, endTime time.Time) error {
	startTimeBytes, endTimeBytes := self.byteArraysForStartAndEndTimes(common.TimeToMicroseconds(startTime), common.TimeToMicroseconds(endTime))
	return self.deleteRangeOfSeriesCommon(database, series, startTimeBytes, endTimeBytes)
}

func (self *LevelDbShard) deleteRangeOfRegex(database string, regex *regexp.Regexp, startTime, endTime time.Time) error {
	series := self.getSeriesForDbAndRegex(database, regex)
	for _, name := range series {
		err := self.deleteRangeOfSeries(database, name, startTime, endTime)
		if err != nil {
			return err
		}
	}
	return nil
}

func (self *LevelDbShard) getFieldsForSeries(db, series string, columns []string) ([]*Field, error) {
	isCountQuery := false
	if len(columns) > 0 && columns[0] == "*" {
		columns = self.getColumnNamesForSeries(db, series)
	} else if len(columns) == 0 {
		isCountQuery = true
		columns = self.getColumnNamesForSeries(db, series)
	}
	if len(columns) == 0 {
		return nil, NewFieldLookupError("Coulnd't look up columns for series: " + series)
	}

	fields := make([]*Field, len(columns), len(columns))

	for i, name := range columns {
		id, errId := self.getIdForDbSeriesColumn(&db, &series, &name)
		if errId != nil {
			return nil, errId
		}
		if id == nil {
			return nil, NewFieldLookupError("Field " + name + " doesn't exist in series " + series)
		}
		fields[i] = &Field{Name: name, Id: id}
	}

	// if it's a count query we just want the column that will be the most efficient to
	// scan through. So find that and return it.
	if isCountQuery {
		bestField := fields[0]
		return []*Field{bestField}, nil
	}
	return fields, nil
}

func (self *LevelDbShard) getColumnNamesForSeries(db, series string) []string {
	it := self.db.NewIterator(self.readOptions)
	defer it.Close()

	seekKey := append(SERIES_COLUMN_INDEX_PREFIX, []byte(db+"~"+series+"~")...)
	it.Seek(seekKey)
	names := make([]string, 0)
	dbNameStart := len(SERIES_COLUMN_INDEX_PREFIX)
	for it = it; it.Valid(); it.Next() {
		key := it.Key()
		if len(key) < dbNameStart || !bytes.Equal(key[:dbNameStart], SERIES_COLUMN_INDEX_PREFIX) {
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
	return names
}

func (self *LevelDbShard) hasReadAccess(querySpec *parser.QuerySpec) bool {
	for series := range querySpec.SeriesValuesAndColumns() {
		if _, isRegex := series.GetCompiledRegex(); !isRegex {
			if !querySpec.HasReadAccess(series.Name) {
				return false
			}
		}
	}
	return true
}

func (self *LevelDbShard) byteArrayForTime(t time.Time) []byte {
	timeBuffer := bytes.NewBuffer(make([]byte, 0, 8))
	timeMicro := common.TimeToMicroseconds(t)
	binary.Write(timeBuffer, binary.BigEndian, self.convertTimestampToUint(&timeMicro))
	return timeBuffer.Bytes()
}

func (self *LevelDbShard) getSeriesForDbAndRegex(database string, regex *regexp.Regexp) []string {
	names := []string{}
	allSeries := self.GetSeriesForDatabase(database)
	for _, name := range allSeries {
		if regex.MatchString(name) {
			names = append(names, name)
		}
	}
	return names
}

func (self *LevelDbShard) GetSeriesForDatabase(database string) []string {
	it := self.db.NewIterator(self.readOptions)
	defer it.Close()

	seekKey := append(DATABASE_SERIES_INDEX_PREFIX, []byte(database+"~")...)
	it.Seek(seekKey)
	dbNameStart := len(DATABASE_SERIES_INDEX_PREFIX)
	names := make([]string, 0)
	for it = it; it.Valid(); it.Next() {
		key := it.Key()
		if len(key) < dbNameStart || !bytes.Equal(key[:dbNameStart], DATABASE_SERIES_INDEX_PREFIX) {
			break
		}
		dbSeries := string(key[dbNameStart:])
		parts := strings.Split(dbSeries, "~")
		if len(parts) > 1 {
			if parts[0] != database {
				break
			}
			name := parts[1]
			names = append(names, name)
		}
	}
	return names
}

func (self *LevelDbShard) createIdForDbSeriesColumn(db, series, column *string) (ret []byte, err error) {
	ret, err = self.getIdForDbSeriesColumn(db, series, column)
	if err != nil {
		return
	}

	if ret != nil {
		return
	}

	self.columnIdMutex.Lock()
	defer self.columnIdMutex.Unlock()
	ret, err = self.getIdForDbSeriesColumn(db, series, column)
	if err != nil {
		return
	}

	if ret != nil {
		return
	}

	ret, err = self.getNextIdForColumn(db, series, column)
	if err != nil {
		return
	}
	s := fmt.Sprintf("%s~%s~%s", *db, *series, *column)
	b := []byte(s)
	key := append(SERIES_COLUMN_INDEX_PREFIX, b...)
	err = self.db.Put(self.writeOptions, key, ret)
	return
}

func (self *LevelDbShard) getIdForDbSeriesColumn(db, series, column *string) (ret []byte, err error) {
	s := fmt.Sprintf("%s~%s~%s", *db, *series, *column)
	b := []byte(s)
	key := append(SERIES_COLUMN_INDEX_PREFIX, b...)
	if ret, err = self.db.Get(self.readOptions, key); err != nil {
		return nil, err
	}
	return ret, nil
}

func (self *LevelDbShard) getNextIdForColumn(db, series, column *string) (ret []byte, err error) {
	id := self.lastIdUsed + 1
	self.lastIdUsed += 1
	idBytes := make([]byte, 8, 8)
	binary.PutUvarint(idBytes, id)
	wb := levigo.NewWriteBatch()
	defer wb.Close()
	wb.Put(NEXT_ID_KEY, idBytes)
	databaseSeriesIndexKey := append(DATABASE_SERIES_INDEX_PREFIX, []byte(*db+"~"+*series)...)
	wb.Put(databaseSeriesIndexKey, []byte{})
	seriesColumnIndexKey := append(SERIES_COLUMN_INDEX_PREFIX, []byte(*db+"~"+*series+"~"+*column)...)
	wb.Put(seriesColumnIndexKey, idBytes)
	if err = self.db.Write(self.writeOptions, wb); err != nil {
		return nil, err
	}
	return idBytes, nil
}

func (self *LevelDbShard) Close() {
	self.closed = true
	self.readOptions.Close()
	self.writeOptions.Close()
	self.db.Close()
}

func (self *LevelDbShard) convertTimestampToUint(t *int64) uint64 {
	if *t < 0 {
		return uint64(math.MaxInt64 + *t + 1)
	}
	return uint64(*t) + uint64(math.MaxInt64) + uint64(1)
}

func (self *LevelDbShard) fetchSinglePoint(querySpec *parser.QuerySpec, series string, fields []*Field) (*protocol.Series, error) {
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
		pointKey := append(field.Id, timeAndSequenceBytes...)

		if data, err := self.db.Get(self.readOptions, pointKey); err != nil {
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

func (self *LevelDbShard) getIterators(fields []*Field, start, end []byte, isAscendingQuery bool) (fieldNames []string, iterators []*levigo.Iterator) {
	iterators = make([]*levigo.Iterator, len(fields))
	fieldNames = make([]string, len(fields))

	// start the iterators to go through the series data
	for i, field := range fields {
		fieldNames[i] = field.Name
		iterators[i] = self.db.NewIterator(self.readOptions)
		if isAscendingQuery {
			iterators[i].Seek(append(field.Id, start...))
		} else {
			iterators[i].Seek(append(append(field.Id, end...), MAX_SEQUENCE...))
			if iterators[i].Valid() {
				iterators[i].Prev()
			}
		}
	}
	return
}

func (self *LevelDbShard) convertUintTimestampToInt64(t *uint64) int64 {
	if *t > uint64(math.MaxInt64) {
		return int64(*t-math.MaxInt64) - int64(1)
	}
	return int64(*t) - math.MaxInt64 - int64(1)
}
