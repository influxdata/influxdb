package datastore

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"common"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/jmhodges/levigo"
	"math"
	"parser"
	"protocol"
	"regexp"
	"strings"
	"sync"
	"time"
)

type LevelDbDatastore struct {
	db            *levigo.DB
	lastIdUsed    uint64
	columnIdMutex sync.Mutex
	readOptions   *levigo.ReadOptions
	writeOptions  *levigo.WriteOptions
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

const (
	ONE_MEGABYTE              = 1024 * 1024
	ONE_GIGABYTE              = ONE_MEGABYTE * 1024
	TWO_FIFTY_SIX_KILOBYTES   = 256 * 1024
	BLOOM_FILTER_BITS_PER_KEY = 64
	MAX_POINTS_TO_SCAN        = 1000000
	MAX_SERIES_SIZE           = ONE_MEGABYTE
)

var (
	// NEXT_ID_KEY holds the next id. ids are used to "intern" timeseries and column names
	NEXT_ID_KEY = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	// SERIES_COLUMN_INDEX_PREFIX is the prefix of the series to column names index
	SERIES_COLUMN_INDEX_PREFIX = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE}
	// DATABASE_SERIES_INDEX_PREFIX is the prefix of the database to series names index
	DATABASE_SERIES_INDEX_PREFIX = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	MAX_SEQUENCE                 = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
)

func NewLevelDbDatastore(dbDir string) (Datastore, error) {
	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(ONE_GIGABYTE))
	opts.SetCreateIfMissing(true)
	opts.SetBlockSize(TWO_FIFTY_SIX_KILOBYTES)
	filter := levigo.NewBloomFilter(BLOOM_FILTER_BITS_PER_KEY)
	opts.SetFilterPolicy(filter)
	db, err := levigo.Open(dbDir, opts)
	if err != nil {
		return nil, err
	}

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

	wo := levigo.NewWriteOptions()

	return &LevelDbDatastore{db: db, lastIdUsed: lastId, readOptions: ro, writeOptions: wo}, nil
}

func (self *LevelDbDatastore) WriteSeriesData(database string, series *protocol.Series) error {
	wb := levigo.NewWriteBatch()
	defer wb.Close()
	for fieldIndex, field := range series.Fields {
		temp := field
		id, _, err := self.getIdForDbSeriesColumn(&database, series.Name, &temp)
		if err != nil {
			return err
		}
		for _, point := range series.Points {
			timestampBuffer := bytes.NewBuffer(make([]byte, 0, 8))
			sequenceNumberBuffer := bytes.NewBuffer(make([]byte, 0, 8))
			binary.Write(timestampBuffer, binary.BigEndian, self.convertTimestampToUint(point.GetTimestampInMicroseconds()))
			binary.Write(sequenceNumberBuffer, binary.BigEndian, uint64(*point.SequenceNumber))
			pointKey := append(append(id, timestampBuffer.Bytes()...), sequenceNumberBuffer.Bytes()...)

			// TODO: we should remove the column value if timestamp and sequence number
			// were provided
			if point.Values[fieldIndex] == nil {
				continue
			}

			data, err2 := proto.Marshal(point.Values[fieldIndex])
			if err2 != nil {
				return err2
			}
			wb.Put(pointKey, data)
		}
	}
	return self.db.Write(self.writeOptions, wb)
}

func (self *LevelDbDatastore) dropSeries(database, series string) error {
	startTimeBytes := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	endTimeBytes := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}

	wb := levigo.NewWriteBatch()
	defer wb.Close()

	for _, name := range self.getColumnNamesForSeries(database, series) {
		if err := self.deleteRangeOfSeries(database, series, startTimeBytes, endTimeBytes); err != nil {
			return err
		}

		indexKey := append(SERIES_COLUMN_INDEX_PREFIX, []byte(database+"~"+series+"~"+name)...)
		wb.Delete(indexKey)
	}

	// remove the column indeces for this time series
	return self.db.Write(self.writeOptions, wb)
}

func (self *LevelDbDatastore) DropDatabase(database string) error {
	wb := levigo.NewWriteBatch()
	defer wb.Close()

	err := self.getSeriesForDb(database, func(name string) error {
		if err := self.dropSeries(database, name); err != nil {
			return err
		}

		seriesKey := append(DATABASE_SERIES_INDEX_PREFIX, []byte(database+"~")...)
		wb.Delete(seriesKey)
		return nil
	})

	if err != nil {
		return err
	}

	return self.db.Write(self.writeOptions, wb)
}

func (self *LevelDbDatastore) ExecuteQuery(user common.User, database string, query *parser.Query, yield func(*protocol.Series) error) error {
	seriesAndColumns := query.GetReferencedColumns()
	hasAccess := true
	for series, columns := range seriesAndColumns {
		if regex, ok := series.GetCompiledRegex(); ok {
			seriesNames := self.getSeriesForDbAndRegex(database, regex)
			for _, name := range seriesNames {
				if !user.HasReadAccess(name) {
					hasAccess = false
					continue
				}
				err := self.executeQueryForSeries(database, name, columns, query, yield)
				if err != nil {
					return err
				}
			}
		} else {
			if !user.HasReadAccess(series.Name) {
				hasAccess = false
				continue
			}
			err := self.executeQueryForSeries(database, series.Name, columns, query, yield)
			if err != nil {
				return err
			}
		}
	}
	if !hasAccess {
		return fmt.Errorf("You don't have permission to access one or more time series")
	}
	return nil
}

func (self *LevelDbDatastore) Close() {
	self.db.Close()
	self.db = nil
	self.readOptions.Close()
	self.readOptions = nil
	self.writeOptions.Close()
	self.writeOptions = nil
}

func (self *LevelDbDatastore) deleteRangeOfSeries(database, series string, startTimeBytes, endTimeBytes []byte) error {
	columns := self.getColumnNamesForSeries(database, series)
	fields, err := self.getFieldsForSeries(database, series, columns)
	if err != nil {
		return err
	}
	ro := levigo.NewReadOptions()
	defer ro.Close()
	ro.SetFillCache(false)
	rangesToCompact := make([]*levigo.Range, 0)
	for _, field := range fields {
		it := self.db.NewIterator(ro)
		defer it.Close()
		wb := levigo.NewWriteBatch()
		defer wb.Close()

		startKey := append(field.Id, startTimeBytes...)
		endKey := startKey
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
			endKey = k
		}
		err = self.db.Write(self.writeOptions, wb)
		if err != nil {
			return err
		}
		rangesToCompact = append(rangesToCompact, &levigo.Range{startKey, endKey})
	}
	for _, r := range rangesToCompact {
		self.db.CompactRange(*r)
	}
	return nil
}

func (self *LevelDbDatastore) DeleteRangeOfSeries(database, series string, startTime, endTime time.Time) error {
	startTimeBytes, endTimeBytes := self.byteArraysForStartAndEndTimes(common.TimeToMicroseconds(startTime), common.TimeToMicroseconds(endTime))
	return self.deleteRangeOfSeries(database, series, startTimeBytes, endTimeBytes)
}

func (self *LevelDbDatastore) DeleteRangeOfRegex(user common.User, database string, regex *regexp.Regexp, startTime, endTime time.Time) error {
	series := self.getSeriesForDbAndRegex(database, regex)
	hasAccess := true
	for _, name := range series {
		if !user.HasWriteAccess(name) {
			hasAccess = false
			continue
		}

		err := self.DeleteRangeOfSeries(database, name, startTime, endTime)
		if err != nil {
			return err
		}
	}
	if !hasAccess {
		return fmt.Errorf("You don't have access to delete from one or more time series")
	}
	return nil
}

func (self *LevelDbDatastore) byteArraysForStartAndEndTimes(startTime, endTime int64) ([]byte, []byte) {
	startTimeBuffer := bytes.NewBuffer(make([]byte, 0, 8))
	binary.Write(startTimeBuffer, binary.BigEndian, self.convertTimestampToUint(&startTime))
	startTimeBytes := startTimeBuffer.Bytes()
	endTimeBuffer := bytes.NewBuffer(make([]byte, 0, 8))
	binary.Write(endTimeBuffer, binary.BigEndian, self.convertTimestampToUint(&endTime))
	endTimeBytes := endTimeBuffer.Bytes()
	return startTimeBytes, endTimeBytes
}

func (self *LevelDbDatastore) getIterators(fields []*Field, start, end []byte, isAscendingQuery bool) (fieldNames []string, iterators []*levigo.Iterator) {
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

// returns true if the point has the correct field id and is
// in the given time range
func isPointInRange(fieldId, startTime, endTime, point []byte) bool {
	id := point[:8]
	time := point[8:16]
	return bytes.Equal(id, fieldId) && bytes.Compare(time, startTime) > -1 && bytes.Compare(time, endTime) < 1
}

func (self *LevelDbDatastore) executeQueryForSeries(database, series string, columns []string, query *parser.Query, yield func(*protocol.Series) error) error {
	startTimeBytes, endTimeBytes := self.byteArraysForStartAndEndTimes(common.TimeToMicroseconds(query.GetStartTime()), common.TimeToMicroseconds(query.GetEndTime()))

	fields, err := self.getFieldsForSeries(database, series, columns)
	if err != nil {
		return err
	}
	fieldCount := len(fields)
	fieldNames, iterators := self.getIterators(fields, startTimeBytes, endTimeBytes, query.Ascending)

	// iterators :=

	result := &protocol.Series{Name: &series, Fields: fieldNames, Points: make([]*protocol.Point, 0)}
	rawColumnValues := make([]*rawColumnValue, fieldCount, fieldCount)

	limit := query.Limit
	if limit == 0 {
		limit = MAX_POINTS_TO_SCAN
	}

	resultByteCount := 0

	// TODO: clean up, this is super gnarly
	// optimize for the case where we're pulling back only a single column or aggregate
	for {
		isValid := false

		point := &protocol.Point{Values: make([]*protocol.FieldValue, fieldCount, fieldCount)}
		for i, it := range iterators {
			if rawColumnValues[i] != nil || !it.Valid() {
				continue
			}

			key := it.Key()
			if len(key) < 16 {
				continue
			}

			if !isPointInRange(fields[i].Id, startTimeBytes, endTimeBytes, key) {
				continue
			}

			time := key[8:16]
			value := it.Value()
			sequenceNumber := key[16:]

			rawValue := &rawColumnValue{time: time, sequence: sequenceNumber, value: value}
			rawColumnValues[i] = rawValue
		}

		var pointTimeRaw []byte
		var pointSequenceRaw []byte
		// choose the highest (or lowest in case of ascending queries) timestamp
		// and sequence number. that will become the timestamp and sequence of
		// the next point.
		for _, value := range rawColumnValues {
			if value == nil {
				continue
			}

			pointTimeRaw, pointSequenceRaw = value.updatePointTimeAndSequence(pointTimeRaw,
				pointSequenceRaw, query.Ascending)
		}

		for i, iterator := range iterators {
			// if the value is nil, or doesn't match the point's timestamp and sequence number
			// then skip it
			if rawColumnValues[i] == nil ||
				!bytes.Equal(rawColumnValues[i].time, pointTimeRaw) ||
				!bytes.Equal(rawColumnValues[i].sequence, pointSequenceRaw) {

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
			err := proto.Unmarshal(rawColumnValues[i].value, fv)
			if err != nil {
				return err
			}
			resultByteCount += len(rawColumnValues[i].value)
			point.Values[i] = fv
			var t uint64
			binary.Read(bytes.NewBuffer(rawColumnValues[i].time), binary.BigEndian, &t)
			time := self.convertUintTimestampToInt64(&t)
			var sequence uint64
			binary.Read(bytes.NewBuffer(rawColumnValues[i].sequence), binary.BigEndian, &sequence)
			seq32 := uint32(sequence)
			point.SetTimestampInMicroseconds(time)
			point.SequenceNumber = &seq32
			rawColumnValues[i] = nil
		}

		// stop the loop if we ran out of points
		if !isValid {
			break
		}

		limit -= 1
		result.Points = append(result.Points, point)

		// add byte count for the timestamp and the sequence
		resultByteCount += 16

		// check if we should send the batch along
		if resultByteCount > MAX_SERIES_SIZE || limit < 1 {
			dropped, err := self.sendBatch(query, result, yield)
			if err != nil {
				return err
			}
			limit += dropped
			resultByteCount = 0
			result = &protocol.Series{Name: &series, Fields: fieldNames, Points: make([]*protocol.Point, 0)}
		}
		if limit < 1 {
			break
		}
	}
	if _, err := self.sendBatch(query, result, yield); err != nil {
		return err
	}
	emptyResult := &protocol.Series{Name: &series, Fields: fieldNames, Points: nil}
	_, err = self.sendBatch(query, emptyResult, yield)
	return err
}

// Return the number of dropped ticks from filtering. if the series
// had more than one alias, returns the min of all dropped ticks
func (self *LevelDbDatastore) sendBatch(query *parser.Query, series *protocol.Series, yield func(series *protocol.Series) error) (int, error) {
	dropped := int(math.MaxInt32)

	for _, alias := range query.GetTableAliases(*series.Name) {
		_alias := alias
		newSeries := &protocol.Series{Name: &_alias, Points: series.Points, Fields: series.Fields}

		lengthBeforeFiltering := len(newSeries.Points)
		var filteredResult *protocol.Series
		if query.GetFromClause().Type == parser.FromClauseInnerJoin {
			filteredResult = newSeries
		} else {
			filteredResult, _ = Filter(query, newSeries)
		}
		_dropped := lengthBeforeFiltering - len(filteredResult.Points)
		if _dropped < dropped {
			dropped = _dropped
		}
		if err := yield(filteredResult); err != nil {
			return 0, err
		}
	}

	return dropped, nil
}

func (self *LevelDbDatastore) getSeriesForDb(database string, yield func(string) error) error {
	it := self.db.NewIterator(self.readOptions)
	defer it.Close()

	seekKey := append(DATABASE_SERIES_INDEX_PREFIX, []byte(database+"~")...)
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
			if err := yield(name); err != nil {
				return err
			}
		}
	}
	return nil
}

func (self *LevelDbDatastore) getSeriesForDbAndRegex(database string, regex *regexp.Regexp) []string {
	names := []string{}
	self.getSeriesForDb(database, func(name string) error {
		if regex.MatchString(name) {
			names = append(names, name)
		}
		return nil
	})
	return names
}

func (self *LevelDbDatastore) getColumnNamesForSeries(db, series string) []string {
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

func (self *LevelDbDatastore) getFieldsForSeries(db, series string, columns []string) ([]*Field, error) {
	isCountQuery := false
	if len(columns) > 0 && columns[0] == "*" {
		columns = self.getColumnNamesForSeries(db, series)
	} else if len(columns) == 0 {
		isCountQuery = true
		columns = self.getColumnNamesForSeries(db, series)
	}
	if len(columns) == 0 {
		return nil, errors.New("Couldn't look up columns for series: " + series)
	}

	fields := make([]*Field, len(columns), len(columns))

	for i, name := range columns {
		id, alreadyPresent, errId := self.getIdForDbSeriesColumn(&db, &series, &name)
		if errId != nil {
			return nil, errId
		}
		if !alreadyPresent {
			return nil, errors.New("Field " + name + " doesn't exist in series " + series)
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

func (self *LevelDbDatastore) getIdForDbSeriesColumn(db, series, column *string) (ret []byte, alreadyPresent bool, err error) {
	s := fmt.Sprintf("%s~%s~%s", *db, *series, *column)
	b := []byte(s)
	key := append(SERIES_COLUMN_INDEX_PREFIX, b...)
	if ret, err = self.db.Get(self.readOptions, key); err != nil {
		return nil, false, err
	}
	if ret == nil {
		ret, err = self.getNextIdForColumn(db, series, column)
		if err = self.db.Put(self.writeOptions, key, ret); err != nil {
			return nil, false, err
		}
		return ret, false, nil
	}
	return ret, true, nil
}

func (self *LevelDbDatastore) getNextIdForColumn(db, series, column *string) (ret []byte, err error) {
	self.columnIdMutex.Lock()
	defer self.columnIdMutex.Unlock()
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

func (self *LevelDbDatastore) convertTimestampToUint(t *int64) uint64 {
	if *t < 0 {
		return uint64(math.MaxInt64 + *t + 1)
	}
	return uint64(*t) + uint64(math.MaxInt64) + uint64(1)
}

func (self *LevelDbDatastore) convertUintTimestampToInt64(t *uint64) int64 {
	if *t > uint64(math.MaxInt64) {
		return int64(*t-math.MaxInt64) - int64(1)
	}
	return int64(*t) - math.MaxInt64 - int64(1)
}
