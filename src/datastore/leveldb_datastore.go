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

const (
	ONE_MEGABYTE              = 1024 * 1024
	ONE_GIGABYTE              = ONE_MEGABYTE * 1024
	TWO_FIFTY_SIX_KILOBYTES   = 256 * 1024
	BLOOM_FILTER_BITS_PER_KEY = 64
	MAX_POINTS_TO_SCAN        = 1000000
	MAX_SERIES_SIZE           = ONE_MEGABYTE
)

var (
	NEXT_ID_KEY                      = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	SERIES_COLUMN_INDEX_PREFIX       = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFD}
	SERIES_COLUMN_DEFINITIONS_PREFIX = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE}
	DATABASE_SERIES_INDEX_PREFIX     = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	MAX_TIMESTAMP_AND_SEQUENCE       = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	MIN_TIMESTAMP_AND_SEQUENCE       = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	MAX_SEQUENCE                     = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	END_KEYSPACE_MARKER              = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
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
			data, err2 := proto.Marshal(point.Values[fieldIndex])
			if err2 != nil {
				return err2
			}
			wb.Put(pointKey, data)
		}
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

func (self *LevelDbDatastore) DeleteRangeOfSeries(database, series string, startTime, endTime time.Time) error {
	columns := self.getColumnNamesForSeries(database, series)
	fields, err := self.getFieldsForSeries(database, series, columns)
	if err != nil {
		return err
	}
	startTimeBytes, endTimeBytes := self.byteArraysForStartAndEndTimes(common.TimeToMicroseconds(startTime), common.TimeToMicroseconds(endTime))
	ro := levigo.NewReadOptions()
	defer ro.Close()
	ro.SetFillCache(false)
	rangesToCompact := make([]*levigo.Range, 0)
	for _, field := range fields {
		it := self.db.NewIterator(ro)
		defer it.Close()
		wb := levigo.NewWriteBatch()

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
	if startTime < 1382361894000 {
		panic("wtf")
	}

	startTimeBuffer := bytes.NewBuffer(make([]byte, 0, 8))
	binary.Write(startTimeBuffer, binary.BigEndian, self.convertTimestampToUint(&startTime))
	startTimeBytes := startTimeBuffer.Bytes()
	endTimeBuffer := bytes.NewBuffer(make([]byte, 0, 8))
	binary.Write(endTimeBuffer, binary.BigEndian, self.convertTimestampToUint(&endTime))
	endTimeBytes := endTimeBuffer.Bytes()
	return startTimeBytes, endTimeBytes
}

func (self *LevelDbDatastore) executeQueryForSeries(database, series string, columns []string, query *parser.Query, yield func(*protocol.Series) error) error {
	startTimeBytes, endTimeBytes := self.byteArraysForStartAndEndTimes(common.TimeToMicroseconds(query.GetStartTime()), common.TimeToMicroseconds(query.GetEndTime()))

	fields, err := self.getFieldsForSeries(database, series, columns)
	if err != nil {
		return err
	}
	fieldCount := len(fields)
	prefixes := make([][]byte, fieldCount, fieldCount)
	iterators := make([]*levigo.Iterator, fieldCount, fieldCount)
	fieldNames := make([]string, len(fields))

	// start the iterators to go through the series data
	for i, field := range fields {
		fieldNames[i] = field.Name
		prefixes[i] = field.Id
		iterators[i] = self.db.NewIterator(self.readOptions)
		if query.Ascending {
			iterators[i].Seek(append(field.Id, startTimeBytes...))
		} else {
			iterators[i].Seek(append(append(field.Id, endTimeBytes...), MAX_SEQUENCE...))
			if iterators[i].Valid() {
				iterators[i].Prev()
			}
		}
	}

	result := &protocol.Series{Name: &series, Fields: fieldNames, Points: make([]*protocol.Point, 0)}
	rawColumnValues := make([]*rawColumnValue, fieldCount, fieldCount)
	isValid := true

	limit := query.Limit
	if limit == 0 {
		limit = MAX_POINTS_TO_SCAN
	}

	resultByteCount := 0

	// TODO: clean up, this is super gnarly
	// optimize for the case where we're pulling back only a single column or aggregate
	for isValid {
		isValid = false
		latestTimeRaw := make([]byte, 8, 8)
		latestSequenceRaw := make([]byte, 8, 8)
		point := &protocol.Point{Values: make([]*protocol.FieldValue, fieldCount, fieldCount)}
		for i, it := range iterators {
			if rawColumnValues[i] == nil && it.Valid() {
				k := it.Key()
				if len(k) >= 16 {
					t := k[8:16]
					if bytes.Equal(k[:8], fields[i].Id) && bytes.Compare(t, startTimeBytes) > -1 && bytes.Compare(t, endTimeBytes) < 1 {
						v := it.Value()
						s := k[16:]
						rawColumnValues[i] = &rawColumnValue{time: t, sequence: s, value: v}
						timeCompare := bytes.Compare(t, latestTimeRaw)
						if timeCompare == 1 {
							latestTimeRaw = t
							latestSequenceRaw = s
						} else if timeCompare == 0 {
							if bytes.Compare(s, latestSequenceRaw) == 1 {
								latestSequenceRaw = s
							}
						}
					}
				}
			}
		}

		for i, iterator := range iterators {
			if rawColumnValues[i] != nil && bytes.Equal(rawColumnValues[i].time, latestTimeRaw) && bytes.Equal(rawColumnValues[i].sequence, latestSequenceRaw) {
				isValid = true
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
		}
		if isValid {
			limit -= 1
			result.Points = append(result.Points, point)

			// add byte count for the timestamp and the sequence
			resultByteCount += 16

			// check if we should send the batch along
			if resultByteCount > MAX_SERIES_SIZE {
				filteredResult, _ := Filter(query, result)
				yield(filteredResult)
				resultByteCount = 0
				result = &protocol.Series{Name: &series, Fields: fieldNames, Points: make([]*protocol.Point, 0)}
			}
		}
		if limit < 1 {
			break
		}
	}
	filteredResult, _ := Filter(query, result)
	yield(filteredResult)
	emptyResult := &protocol.Series{Name: &series, Points: nil}
	yield(emptyResult)
	return nil
}

func (self *LevelDbDatastore) getSeriesForDbAndRegex(database string, regex *regexp.Regexp) []string {
	it := self.db.NewIterator(self.readOptions)
	defer it.Close()

	seekKey := append(DATABASE_SERIES_INDEX_PREFIX, []byte(database+"~")...)
	it.Seek(seekKey)
	names := make([]string, 0)
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
			if regex.MatchString(name) {
				names = append(names, parts[1])
			}
		}
	}
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
		return nil, errors.New("Coulnd't look up columns for series: " + series)
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
