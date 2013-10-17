package datastore

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/jmhodges/levigo"
	"math"
	"parser"
	"protocol"
	"strings"
	"sync"
)

type LevelDbDatastore struct {
	db            *levigo.DB
	lastIdUsed    uint64
	columnIdMutex sync.Mutex
}

type Field struct {
	Id         []byte
	Name       string
	Definition *protocol.FieldDefinition
}

type rawColumnValue struct {
	time     []byte
	sequence []byte
	value    []byte
}

const (
	ONE_GIGABYTE              = 1024 * 1024 * 1024
	TWO_FIFTY_SIX_KILOBYTES   = 256 * 1024
	BLOOM_FILTER_BITS_PER_KEY = 64
	MAX_POINTS_TO_SCAN        = 100000
)

var (
	NEXT_ID_KEY                      = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	SERIES_COLUMN_INDEX_PREFIX       = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFD}
	SERIES_COLUMN_DEFINITIONS_PREFIX = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE}
	DATABASE_SERIES_INDEX_PREFIX     = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	MAX_TIMESTAMP_AND_SEQUENCE       = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	MIN_TIMESTAMP_AND_SEQUENCE       = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	MAX_SEQUENCE                     = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
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
	defer ro.Close()

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

	return &LevelDbDatastore{db: db, lastIdUsed: lastId}, nil
}

func (self *LevelDbDatastore) WriteSeriesData(database string, series *protocol.Series) error {
	wo := levigo.NewWriteOptions()
	wb := levigo.NewWriteBatch()
	defer wo.Close()
	defer wb.Close()
	for fieldIndex, field := range series.Fields {
		id, alreadyPresent, err := self.getIdForDbSeriesColumn(&database, series.Name, field.Name)
		if err != nil {
			return err
		}
		if !alreadyPresent {
			d, e := proto.Marshal(field)
			if e != nil {
				return e
			}
			wb.Put(append(SERIES_COLUMN_DEFINITIONS_PREFIX, id...), d)
		}
		for _, point := range series.Points {
			timestampBuffer := bytes.NewBuffer(make([]byte, 0, 8))
			sequenceNumberBuffer := bytes.NewBuffer(make([]byte, 0, 8))
			binary.Write(timestampBuffer, binary.BigEndian, self.convertTimestampToUint(point.Timestamp))
			binary.Write(sequenceNumberBuffer, binary.BigEndian, uint64(*point.SequenceNumber))
			pointKey := append(append(id, timestampBuffer.Bytes()...), sequenceNumberBuffer.Bytes()...)
			data, err2 := proto.Marshal(point.Values[fieldIndex])
			if err2 != nil {
				return err2
			}
			wb.Put(pointKey, data)
		}
	}
	return self.db.Write(wo, wb)
}

func (self *LevelDbDatastore) ExecuteQuery(database string, query *parser.Query, yield func(*protocol.Series) error) error {
	seriesAndColumns := query.GetReferencedColumns()
	for series, columns := range seriesAndColumns {
		err := self.executeQueryForSeries(database, series, columns, query, yield)
		if err != nil {
			return err
		}
	}
	return nil
}

func (self *LevelDbDatastore) Close() {
	self.db.Close()
}

func (self *LevelDbDatastore) executeQueryForSeries(database, series string, columns []string, query *parser.Query, yield func(*protocol.Series) error) error {
	startTime := query.GetStartTime().Unix()
	startTimeBuffer := bytes.NewBuffer(make([]byte, 0, 8))
	binary.Write(startTimeBuffer, binary.BigEndian, self.convertTimestampToUint(&startTime))
	startTimeBytes := startTimeBuffer.Bytes()
	endTime := query.GetEndTime().Unix()
	endTimeBuffer := bytes.NewBuffer(make([]byte, 0, 8))
	binary.Write(endTimeBuffer, binary.BigEndian, self.convertTimestampToUint(&endTime))
	endTimeBytes := endTimeBuffer.Bytes()
	fields, err := self.getFieldsForSeries(database, series, columns)
	if err != nil {
		return err
	}
	fieldCount := len(fields)
	prefixes := make([][]byte, fieldCount, fieldCount)
	iterators := make([]*levigo.Iterator, fieldCount, fieldCount)
	fieldDefinitions := make([]*protocol.FieldDefinition, fieldCount, fieldCount)

	// start the iterators to go through the series data
	for i, field := range fields {
		fieldDefinitions[i] = field.Definition
		prefixes[i] = field.Id
		ro := levigo.NewReadOptions()
		defer ro.Close()
		iterators[i] = self.db.NewIterator(ro)
		iterators[i].Seek(append(append(field.Id, endTimeBytes...), MAX_SEQUENCE...))
		iterators[i].Prev()
	}

	result := &protocol.Series{Name: &series, Fields: fieldDefinitions, Points: make([]*protocol.Point, 0)}
	rawColumnValues := make([]*rawColumnValue, fieldCount, fieldCount)
	isValid := true

	limit := query.Limit
	if limit == 0 {
		limit = MAX_POINTS_TO_SCAN
	}

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
					if bytes.Equal(k[:8], fields[i].Id) && bytes.Compare(t, startTimeBytes) == 1 {
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
				iterator.Prev()
				fv := &protocol.FieldValue{}
				err := proto.Unmarshal(rawColumnValues[i].value, fv)
				if err != nil {
					return err
				}
				point.Values[i] = fv
				var t uint64
				binary.Read(bytes.NewBuffer(rawColumnValues[i].time), binary.BigEndian, &t)
				time := self.convertUintTimestampToInt64(&t)
				var sequence uint64
				binary.Read(bytes.NewBuffer(rawColumnValues[i].sequence), binary.BigEndian, &sequence)
				seq32 := uint32(sequence)
				point.Timestamp = &time
				point.SequenceNumber = &seq32
				rawColumnValues[i] = nil
			}
		}
		if isValid {
			limit -= 1
			result.Points = append(result.Points, point)
		}
		if limit < 1 {
			break
		}
	}
	filteredResult, _ := Filter(query, result)
	yield(filteredResult)
	return nil
}

func (self *LevelDbDatastore) getColumnNamesForSeries(db, series string) []string {
	ro := levigo.NewReadOptions()
	defer ro.Close()
	it := self.db.NewIterator(ro)
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
	ro := levigo.NewReadOptions()
	defer ro.Close()

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
		key := append(SERIES_COLUMN_DEFINITIONS_PREFIX, id...)
		data, err := self.db.Get(ro, key)
		if err != nil {
			return nil, err
		}
		fd := &protocol.FieldDefinition{}
		err = proto.Unmarshal(data, fd)
		if err != nil {
			return nil, err
		}
		fields[i] = &Field{Name: name, Definition: fd, Id: id}
	}

	// if it's a count query we just want the column that will be the most efficient to
	// scan through. So find that and return it.
	if isCountQuery {
		bestField := fields[0]
		for _, f := range fields {
			if *f.Definition.Type == protocol.FieldDefinition_BOOL {
				bestField = f
				break
			} else if *f.Definition.Type == protocol.FieldDefinition_INT32 {
				bestField = f
			} else if *f.Definition.Type == protocol.FieldDefinition_INT64 && *bestField.Definition.Type != protocol.FieldDefinition_INT32 {
				bestField = f
			} else if *f.Definition.Type == protocol.FieldDefinition_DOUBLE && *bestField.Definition.Type != protocol.FieldDefinition_INT32 && *bestField.Definition.Type != protocol.FieldDefinition_INT64 {
				bestField = f
			}
		}
		return []*Field{bestField}, nil
	}
	return fields, nil
}

func (self *LevelDbDatastore) getIdForDbSeriesColumn(db, series, column *string) (ret []byte, alreadyPresent bool, err error) {
	s := fmt.Sprintf("%s~%s~%s", *db, *series, *column)
	b := []byte(s)
	key := append(SERIES_COLUMN_INDEX_PREFIX, b...)
	ro := levigo.NewReadOptions()
	defer ro.Close()
	if ret, err = self.db.Get(ro, key); err != nil {
		return nil, false, err
	}
	if ret == nil {
		ret, err = self.getNextIdForColumn(db, series, column)
		wo := levigo.NewWriteOptions()
		defer wo.Close()
		if err = self.db.Put(wo, key, ret); err != nil {
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
	wo := levigo.NewWriteOptions()
	idBytes := make([]byte, 8, 8)
	binary.PutUvarint(idBytes, id)
	wb := levigo.NewWriteBatch()
	wb.Put(NEXT_ID_KEY, idBytes)
	databaseSeriesIndexKey := append(DATABASE_SERIES_INDEX_PREFIX, []byte(*db+"~"+*series)...)
	wb.Put(databaseSeriesIndexKey, idBytes)
	seriesColumnIndexKey := append(SERIES_COLUMN_INDEX_PREFIX, []byte(*db+"~"+*series+"~"+*column)...)
	wb.Put(seriesColumnIndexKey, idBytes)
	if err = self.db.Write(wo, wb); err != nil {
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
