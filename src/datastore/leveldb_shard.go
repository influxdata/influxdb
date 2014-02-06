package datastore

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	log "code.google.com/p/log4go"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/jmhodges/levigo"
	"math"
	"parser"
	"protocol"
	"sync"
)

type LevelDbShard struct {
	db            *levigo.DB
	readOptions   *levigo.ReadOptions
	writeOptions  *levigo.WriteOptions
	lastIdUsed    uint64
	columnIdMutex sync.Mutex
}

func NewLevelDbShard(db *levigo.DB) (*LevelDbShard, error) {
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
		db:           db,
		writeOptions: levigo.NewWriteOptions(),
		readOptions:  ro,
		lastIdUsed:   lastId,
	}, nil
}

var (
	endStreamResponse = protocol.Response_END_STREAM
)

func (self *LevelDbShard) Write(database string, series *protocol.Series) error {
	wb := levigo.NewWriteBatch()
	defer wb.Close()

	if series == nil || len(series.Points) == 0 {
		return errors.New("Unable to write no data. Series was nil or had no points.")
	}

	for fieldIndex, field := range series.Fields {
		temp := field
		id, err := self.createIdForDbSeriesColumn(&database, series.Name, &temp)
		if err != nil {
			return err
		}
		for _, point := range series.Points {
			keyBuffer := bytes.NewBuffer(make([]byte, 0, 24))
			keyBuffer.Write(id)
			binary.Write(keyBuffer, binary.BigEndian, self.convertTimestampToUint(point.GetTimestampInMicroseconds()))
			binary.Write(keyBuffer, binary.BigEndian, *point.SequenceNumber)
			pointKey := keyBuffer.Bytes()

			if point.Values[fieldIndex].GetIsNull() {
				wb.Delete(pointKey)
				continue
			}

			data, err := proto.Marshal(point.Values[fieldIndex])
			if err != nil {
				return err
			}
			wb.Put(pointKey, data)
		}
	}

	return self.db.Write(self.writeOptions, wb)
}

func (self *LevelDbShard) Query(querySpec *parser.QuerySpec, responseChan chan *protocol.Response) error {
	log.Error("LevelDbShard: QUERY")
	response := &protocol.Response{Type: &endStreamResponse}
	responseChan <- response
	return nil
}

func (self *LevelDbShard) createIdForDbSeriesColumn(db, series, column *string) (ret []byte, err error) {
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

func (self *LevelDbShard) close() {
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
