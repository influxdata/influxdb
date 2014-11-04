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

func (self *Shard) executeSinglePointQuery(querySpec *parser.QuerySpec, series string, fields []*metastore.Field, p engine.Processor) error {
	query := querySpec.SelectQuery()
	fieldCount := len(fields)
	fieldNames := make([]string, 0, fieldCount)
	point := &protocol.Point{Values: make([]*protocol.FieldValue, 0, fieldCount)}
	timestamp := common.TimeToMicroseconds(query.GetStartTime())
	seqno, err := query.GetSinglePointQuerySequenceNumber()
	if err != nil {
		return err
	}

	// set the timestamp and sequence number
	point.SequenceNumber = &seqno
	point.SetTimestampInMicroseconds(timestamp)

	for _, field := range fields {
		sk := newStorageKey(field.Id, timestamp, seqno)
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

type FieldLookupError struct {
	message string
}

func NewFieldLookupError(message string) *FieldLookupError {
	return &FieldLookupError{message}
}

func (self FieldLookupError) Error() string {
	return self.message
}
