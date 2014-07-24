package metastore

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"regexp"
	"sync"

	"github.com/influxdb/influxdb/protocol"
)

type Store struct {
	fieldsLock sync.RWMutex

	// Map of databases > series > fields
	StringsToIds map[string]map[string]map[string]uint64
	// Map of ids to the field names. Don't need to know which database or series
	// they track to since we have that information elsewhere
	LastIdUsed       uint64
	clusterConsensus ClusterConsensus
}

type Field struct {
	Id   uint64
	Name string
}

func (f *Field) String() string {
	return fmt.Sprintf("Name: %s, Id: %d", f.Name, f.Id)
}

func (f *Field) IdAsBytes() []byte {
	idBytes := bytes.NewBuffer(make([]byte, 0, 8))
	binary.Write(idBytes, binary.BigEndian, f.Id)
	return idBytes.Bytes()
}

type ClusterConsensus interface {
	// Will return a collection of series objects that have their name, fields, and fieldIds set.
	// This can be used to set the field ids and fill the cache.
	GetOrSetFieldIdsForSeries(database string, series []*protocol.Series) ([]*protocol.Series, error)
}

func NewStore() *Store {
	return &Store{
		StringsToIds: make(map[string]map[string]map[string]uint64),
	}
}

func NewStoreFromJson(data []byte) (*Store, error) {
	store := &Store{}
	err := json.Unmarshal(data, store)
	return store, err
}

func (self *Store) ToJson() ([]byte, error) {
	return json.Marshal(self)
}

func (self *Store) UpdateFromSnapshot(other *Store) {
	clusterConsensus := self.clusterConsensus
	*self = *other
	self.clusterConsensus = clusterConsensus
}

func (self *Store) SetClusterConsensus(c ClusterConsensus) {
	self.clusterConsensus = c
}

func (self *Store) ReplaceFieldNamesWithFieldIds(database string, series []*protocol.Series) error {
	allSetFromCache := self.setFieldIdsFromCache(database, series)
	if allSetFromCache {
		return nil
	}

	// one or more of the fields weren't in the cache, so just get the whole list from Raft
	seriesWithOnlyFields := make([]*protocol.Series, len(series), len(series))
	for i, s := range series {
		seriesWithOnlyFields[i] = &protocol.Series{Name: s.Name, Fields: s.Fields}
	}
	seriesWithFieldIds, err := self.clusterConsensus.GetOrSetFieldIdsForSeries(database, seriesWithOnlyFields)
	if err != nil {
		return err
	}

	for i, s := range series {
		s.Fields = nil
		s.FieldIds = seriesWithFieldIds[i].FieldIds
	}
	return nil
}

func (self *Store) GetOrSetFieldIds(database string, series []*protocol.Series) error {
	self.fieldsLock.Lock()
	defer self.fieldsLock.Unlock()
	databaseSeries := self.StringsToIds[database]
	if databaseSeries == nil {
		databaseSeries = make(map[string]map[string]uint64)
		self.StringsToIds[database] = databaseSeries
	}
	for _, s := range series {
		fieldIds := make([]uint64, len(s.Fields), len(s.Fields))
		seriesFieldMap := databaseSeries[*s.Name]
		if seriesFieldMap == nil {
			seriesFieldMap = make(map[string]uint64)
			databaseSeries[*s.Name] = seriesFieldMap
		}
		for i, fieldName := range s.Fields {
			fieldId, ok := seriesFieldMap[fieldName]
			if !ok {
				self.LastIdUsed += 1
				seriesFieldMap[fieldName] = self.LastIdUsed
				fieldId = self.LastIdUsed
			}
			fieldIds[i] = fieldId
		}
		s.FieldIds = fieldIds
	}
	return nil
}

func (self *Store) GetSeriesForDatabaseAndRegex(database string, regex *regexp.Regexp) []string {
	self.fieldsLock.RLock()
	defer self.fieldsLock.RUnlock()
	databaseSeries, ok := self.StringsToIds[database]
	if !ok {
		return nil
	}
	matchingSeries := make([]string, 0)
	for series := range databaseSeries {
		if regex.MatchString(series) {
			matchingSeries = append(matchingSeries, series)
		}
	}
	return matchingSeries
}

func (self *Store) GetSeriesForDatabase(database string) []string {
	self.fieldsLock.RLock()
	defer self.fieldsLock.RUnlock()
	databaseSeries, ok := self.StringsToIds[database]
	if !ok {
		return nil
	}
	series := make([]string, 0, len(databaseSeries))
	for s := range databaseSeries {
		series = append(series, s)
	}
	return series
}

func (self *Store) getFieldsForDatabase(database string) []*Field {
	fields := make([]*Field, 0)
	databaseSeries, ok := self.StringsToIds[database]
	if !ok {
		return nil
	}
	for _, series := range databaseSeries {
		for fieldName, fieldId := range series {
			fields = append(fields, &Field{Id: fieldId, Name: fieldName})
		}
	}
	return fields
}

func (self *Store) GetFieldsForDatabase(database string) []*Field {
	self.fieldsLock.RLock()
	defer self.fieldsLock.RUnlock()
	return self.getFieldsForDatabase(database)
}

func (self *Store) getFieldsForSeries(database, series string) []*Field {
	databaseSeries, ok := self.StringsToIds[database]
	if !ok {
		return nil
	}
	fieldMap := databaseSeries[series]
	fields := make([]*Field, 0, len(fieldMap))
	for name, id := range fieldMap {
		fields = append(fields, &Field{Name: name, Id: id})
	}
	return fields
}

func (self *Store) GetFieldsForSeries(database, series string) []*Field {
	self.fieldsLock.RLock()
	defer self.fieldsLock.RUnlock()
	return self.getFieldsForSeries(database, series)
}

func (self *Store) DropSeries(database, series string) ([]*Field, error) {
	self.fieldsLock.Lock()
	defer self.fieldsLock.Unlock()
	fields := self.getFieldsForSeries(database, series)
	databaseSeries := self.StringsToIds[database]
	if databaseSeries != nil {
		delete(databaseSeries, series)
	}
	return fields, nil
}

func (self *Store) DropDatabase(database string) ([]*Field, error) {
	self.fieldsLock.Lock()
	defer self.fieldsLock.Unlock()
	fields := self.getFieldsForDatabase(database)
	delete(self.StringsToIds, database)
	return fields, nil
}

func (self *Store) setFieldIdsFromCache(database string, series []*protocol.Series) bool {
	self.fieldsLock.RLock()
	defer self.fieldsLock.RUnlock()
	databaseSeries := self.StringsToIds[database]
	for _, s := range series {
		seriesFields, ok := databaseSeries[*s.Name]
		if !ok {
			return false
		}
		fieldIds := make([]uint64, len(s.Fields), len(s.Fields))
		for i, f := range s.Fields {
			fieldId, ok := seriesFields[f]
			if !ok {
				return false
			}
			fieldIds[i] = fieldId
		}
		s.FieldIds = fieldIds
	}
	return true
}
