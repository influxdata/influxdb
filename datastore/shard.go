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

	if !self.hasReadAccess(querySpec) {
		return errors.New("User does not have access to one or more of the series requested.")
	}

	switch t := querySpec.SelectQuery().FromClause.Type; t {
	case parser.FromClauseArray:
		log.Debug("Shard %s: running a regular query", self.db.Path())
		return self.executeArrayQuery(querySpec, processor)
	case parser.FromClauseMerge, parser.FromClauseInnerJoin:
		log.Debug("Shard %s: running a merge query", self.db.Path())
		return self.executeMergeQuery(querySpec, processor, t)
	default:
		panic(fmt.Errorf("Unknown from clause type %s", t))
	}
}

func (self *Shard) IsClosed() bool {
	return self.closed
}

func (self *Shard) executeMergeQuery(querySpec *parser.QuerySpec, processor engine.Processor, t parser.FromClauseType) error {
	seriesAndColumns := querySpec.SelectQuery().GetReferencedColumns()
	iterators := make([]*PointIterator, len(seriesAndColumns))
	streams := make([]engine.StreamQuery, len(iterators))
	i := 0
	var err error
	for s, c := range seriesAndColumns {
		c, iterators[i], err = self.getPointIteratorForSeries(querySpec, s.Name, c)
		if err != nil {
			log.Error(err)
			return err
		}
		defer iterators[i].Close()
		aliases := querySpec.SelectQuery().GetTableAliases(s.Name)
		if len(aliases) > 1 {
			return fmt.Errorf("Cannot have the same table joined more than once")
		}
		streams[i] = PointIteratorStream{
			pi:     iterators[i],
			name:   aliases[0],
			fields: c,
		}
		i++
	}

	h := engine.NewSeriesHeap(querySpec.IsAscending())
	merger := engine.NewCME("Shard", streams, h, processor, t == parser.FromClauseMerge)
	if _, err := merger.Update(); err != nil {
		return err
	}
	return nil
}

func (self *Shard) executeArrayQuery(querySpec *parser.QuerySpec, processor engine.Processor) error {
	seriesAndColumns := querySpec.SelectQuery().GetReferencedColumns()

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

func (self *Shard) executeQueryForSeries(querySpec *parser.QuerySpec, name string, columns []string, processor engine.Processor) error {
	if querySpec.IsSinglePointQuery() {
		log.Debug("Running single query for series %s", name)
		return self.executeSinglePointQuery(querySpec, name, columns, processor)
	}
	var pi *PointIterator
	var err error
	columns, pi, err = self.getPointIteratorForSeries(querySpec, name, columns)
	if err != nil {
		return err
	}
	defer pi.Close()

	query := querySpec.SelectQuery()
	aliases := query.GetTableAliases(name)

	seriesOutgoing := &protocol.Series{Name: protocol.String(name), Fields: columns, Points: make([]*protocol.Point, 0, self.pointBatchSize)}
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
				return nil
			}
			seriesOutgoing = &protocol.Series{Name: protocol.String(name), Fields: columns, Points: make([]*protocol.Point, 0, self.pointBatchSize)}
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

func (self *Shard) executeSinglePointQuery(querySpec *parser.QuerySpec, name string, columns []string, p engine.Processor) error {
	fields, err := self.getFieldsForSeries(querySpec.Database(), name, columns)
	if err != nil {
		log.Error("Error looking up fields for %s: %s", name, err)
		return err
	}

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

	result := &protocol.Series{Name: &name, Fields: fieldNames, Points: []*protocol.Point{point}}

	if len(result.Points) > 0 {
		_, err := p.Yield(result)
		return err
	}
	return nil
}

func (self *Shard) getPointIteratorForSeries(querySpec *parser.QuerySpec, name string, columns []string) ([]string, *PointIterator, error) {
	fields, err := self.getFieldsForSeries(querySpec.Database(), name, columns)
	if err != nil {
		log.Error("Error looking up fields for %s: %s", name, err)
		return nil, nil, err
	}

	startTime := querySpec.GetStartTime()
	endTime := querySpec.GetEndTime()

	query := querySpec.SelectQuery()

	iterators := self.getIterators(fields, startTime, endTime, query.Ascending)
	pi := NewPointIterator(iterators, fields, querySpec.GetStartTime(), querySpec.GetEndTime(), query.Ascending)

	columns = make([]string, len(fields))
	for i := range fields {
		columns[i] = fields[i].Name
	}

	return columns, pi, nil
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

// func (self *Shard) compact() {
// 	log.Info("Compacting shard")
// 	self.db.CompactRange(levigo.Range{})
// 	log.Info("Shard compaction is done")
// }

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

func (self *Shard) getIterators(fields []*metastore.Field, start, end time.Time, isAscendingQuery bool) (iterators []storage.Iterator) {
	iterators = make([]storage.Iterator, len(fields))

	// start the iterators to go through the series data
	for i, field := range fields {
		iterators[i] = self.db.Iterator()

		t := start
		var seq uint64 = 0
		if !isAscendingQuery {
			t = end
			seq = maxSeqNumber
		}

		tmicro := common.TimeToMicroseconds(t)
		sk := newStorageKey(field.Id, tmicro, seq)
		log.Debug("Initializing iterator to %v", sk.bytes())
		iterators[i].Seek(sk.bytes())

		if !isAscendingQuery && iterators[i].Valid() {
			iterators[i].Prev()
		}

		if err := iterators[i].Error(); err != nil {
			log.Error("Error while getting iterators: %s", err)
			return nil
		}
	}
	return
}

func (self *Shard) getFieldsForSeries(db, series string, columns []string) ([]*metastore.Field, error) {
	allFields := self.metaStore.GetFieldsForSeries(db, series)
	if len(allFields) == 0 {
		return nil, FieldLookupError{"Couldn't find series: " + series}
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
