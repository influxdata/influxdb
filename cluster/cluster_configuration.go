package cluster

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"regexp"
	"sort"
	"sync"
	"time"

	log "code.google.com/p/log4go"
	"github.com/influxdb/influxdb/common"
	"github.com/influxdb/influxdb/configuration"
	"github.com/influxdb/influxdb/metastore"
	"github.com/influxdb/influxdb/parser"
	"github.com/influxdb/influxdb/protocol"
)

// defined by cluster config (in cluster package)
type QuerySpec interface {
	GetStartTime() time.Time
	GetEndTime() time.Time
	Database() string
	TableNames() []string
	TableNamesAndRegex() ([]string, *regexp.Regexp)
	GetGroupByInterval() *time.Duration
	AllShardsQuery() bool
	IsRegex() bool
}

func (self *ClusterConfiguration) convertShardsToNewShardData(shards []*ShardData) []*NewShardData {
	newShardData := make([]*NewShardData, len(shards))
	for i, shard := range shards {
		newShardData[i] = &NewShardData{
			Id:        shard.id,
			Database:  shard.Database,
			SpaceName: shard.SpaceName,
			StartTime: shard.startTime,
			EndTime:   shard.endTime,
			ServerIds: shard.serverIds}
	}
	return newShardData
}

func (self *ClusterConfiguration) convertNewShardDataToShards(newShards []*NewShardData) []*ShardData {
	shards := make([]*ShardData, len(newShards))
	for i, newShard := range newShards {
		shard := NewShard(newShard.Id, newShard.StartTime, newShard.EndTime, newShard.Database, newShard.SpaceName, self.wal)
		servers := make([]*ClusterServer, 0)
		for _, serverId := range newShard.ServerIds {
			if serverId == self.LocalServer.Id {
				err := shard.SetLocalStore(self.shardStore, self.LocalServer.Id)
				if err != nil {
					log.Error("CliusterConfig convertNewShardDataToShards: ", err)
				}
			} else {
				server := self.GetServerById(&serverId)
				servers = append(servers, server)
			}
		}
		shard.SetServers(servers)
		shards[i] = shard
	}
	return shards
}

func (self *ClusterConfiguration) GetShardToWriteToBySeriesAndTime(db, series string, microsecondsEpoch int64) (*ShardData, error) {
	shardSpace := self.getShardSpaceToMatchSeriesName(db, series)
	if shardSpace == nil {
		var err error
		shardSpace, err = self.createDefaultShardSpace(db)
		if err != nil {
			return nil, err
		}
	}

	// if the shard will be dropped anyway because of the shard space
	// retention period, then return nothing. Don't try to write
	retention := shardSpace.ParsedRetentionPeriod()
	if retention != InfiniteRetention {
		_, endTime := self.getStartAndEndBasedOnDuration(microsecondsEpoch, shardSpace.SecondsOfDuration())
		if endTime.Before(time.Now().Add(-retention)) {
			return nil, nil
		}
	}

	matchingShards := make([]*ShardData, 0)
	for _, s := range shardSpace.shards {
		if s.IsMicrosecondInRange(microsecondsEpoch) {
			matchingShards = append(matchingShards, s)
		} else if len(matchingShards) > 0 {
			// shards are always in time descending order. If we've already found one and the next one doesn't match, we can ignore the rest
			break
		}
	}

	var err error
	if len(matchingShards) == 0 {
		log.Info("No matching shards for write at time %du, creating...", microsecondsEpoch)
		matchingShards, err = self.createShards(microsecondsEpoch, shardSpace)
		if err != nil {
			return nil, err
		}
	}

	if len(matchingShards) == 1 {
		return matchingShards[0], nil
	}

	index := HashDbAndSeriesToInt(db, series)
	index = index % len(matchingShards)
	return matchingShards[index], nil
}

func (self *ClusterConfiguration) createShards(microsecondsEpoch int64, shardSpace *ShardSpace) ([]*ShardData, error) {
	startIndex := 0
	if self.lastServerToGetShard != nil {
		for i, server := range self.servers {
			if server == self.lastServerToGetShard {
				startIndex = i + 1
			}
		}
	}

	shards := make([]*NewShardData, 0)
	startTime, endTime := self.getStartAndEndBasedOnDuration(microsecondsEpoch, shardSpace.SecondsOfDuration())

	log.Info("createShards for space %s: start: %s. end: %s",
		shardSpace.Name,
		startTime.Format("Mon Jan 2 15:04:05 -0700 MST 2006"), endTime.Format("Mon Jan 2 15:04:05 -0700 MST 2006"))

	for i := shardSpace.Split; i > 0; i-- {
		serverIds := make([]uint32, 0)

		// if they have the replication factor set higher than the number of servers in the cluster, limit it
		rf := int(shardSpace.ReplicationFactor)
		if rf > len(self.servers) {
			rf = len(self.servers)
		}

		for ; rf > 0; rf-- {
			if startIndex >= len(self.servers) {
				startIndex = 0
			}
			server := self.servers[startIndex]
			self.lastServerToGetShard = server
			serverIds = append(serverIds, server.Id)
			startIndex += 1
		}
		shards = append(shards, &NewShardData{
			StartTime: *startTime,
			EndTime:   *endTime,
			ServerIds: serverIds,
			Database:  shardSpace.Database,
			SpaceName: shardSpace.Name})
	}

	// call out to rafter server to create the shards (or return shard objects that the leader already knows about)
	createdShards, err := self.shardCreator.CreateShards(shards)
	if err != nil {
		return nil, err
	}
	return createdShards, nil
}

func (self *ClusterConfiguration) getStartAndEndBasedOnDuration(microsecondsEpoch int64, duration float64) (*time.Time, *time.Time) {
	startTimeSeconds := math.Floor(float64(microsecondsEpoch)/1000.0/1000.0/duration) * duration
	startTime := time.Unix(int64(startTimeSeconds), 0)
	endTime := time.Unix(int64(startTimeSeconds+duration), 0)

	return &startTime, &endTime
}

func (self *ClusterConfiguration) GetShardsForQuery(querySpec *parser.QuerySpec) (Shards, error) {
	shards, err := self.getShardsToMatchQuery(querySpec)
	if err != nil {
		return nil, err
	}
	log.Debug("Querying %d shards for query", len(shards))
	shards = self.getShardRange(querySpec, shards)
	if querySpec.IsAscending() {
		SortShardsByTimeAscending(shards)
	}
	return shards, nil
}

func (self *ClusterConfiguration) getShardsToMatchQuery(querySpec *parser.QuerySpec) ([]*ShardData, error) {
	self.shardLock.RLock()
	defer self.shardLock.RUnlock()
	seriesNames, fromRegex := querySpec.TableNamesAndRegex()
	db := querySpec.Database()
	if fromRegex != nil {
		seriesNames = self.MetaStore.GetSeriesForDatabaseAndRegex(db, fromRegex)
	}
	uniqueShards := make(map[uint32]*ShardData)
	for _, name := range seriesNames {
		if fs := self.MetaStore.GetFieldsForSeries(db, name); len(fs) == 0 {
			return nil, fmt.Errorf("Couldn't find series: %s", name)
		}
		space := self.getShardSpaceToMatchSeriesName(db, name)
		if space == nil {
			continue
		}
		for _, shard := range space.shards {
			uniqueShards[shard.id] = shard
		}
	}
	shards := make([]*ShardData, 0, len(uniqueShards))
	for _, shard := range uniqueShards {
		shards = append(shards, shard)
	}
	SortShardsByTimeDescending(shards)
	return shards, nil
}

func (self *ClusterConfiguration) getShardSpaceToMatchSeriesName(database, name string) *ShardSpace {
	// order of matching for any series. First look at the database specific shard
	// spaces. Then look at the defaults.
	databaseSpaces := self.databaseShardSpaces[database]
	if databaseSpaces == nil {
		return nil
	}
	for _, s := range databaseSpaces {
		if s.MatchesSeries(name) {
			return s
		}
	}
	return nil
}

func (self *ClusterConfiguration) getShardRange(querySpec QuerySpec, shards []*ShardData) []*ShardData {
	if querySpec.AllShardsQuery() {
		return shards
	}

	startTime := common.TimeToMicroseconds(querySpec.GetStartTime())
	endTime := common.TimeToMicroseconds(querySpec.GetEndTime())

	// the shards are always in descending order, if we have the following shards
	// [t + 20, t + 30], [t + 10, t + 20], [t, t + 10]
	// if we are querying [t + 5, t + 15], we have to find the first shard whose
	// startMicro is less than the end time of the query,
	// which is the second shard [t + 10, t + 20], then
	// start searching from this shard for the shard that has
	// endMicro less than the start time of the query, which is
	// no entry (sort.Search will return the length of the slice
	// in this case) so we return [t + 10, t + 20], [t, t + 10]
	// as expected

	startIndex := sort.Search(len(shards), func(n int) bool {
		return shards[n].startMicro < endTime
	})

	if startIndex == len(shards) {
		return nil
	}

	endIndex := sort.Search(len(shards)-startIndex, func(n int) bool {
		return shards[n+startIndex].endMicro <= startTime
	})

	return shards[startIndex : endIndex+startIndex]
}

func HashDbAndSeriesToInt(database, series string) int {
	hasher := sha1.New()
	hasher.Write([]byte(fmt.Sprintf("%s%s", database, series)))
	buf := bytes.NewBuffer(hasher.Sum(nil))
	var n int64
	binary.Read(buf, binary.LittleEndian, &n)
	nInt := int(n)
	if nInt < 0 {
		nInt = nInt * -1
	}
	return nInt
}

func areShardsEqual(s1 *ShardData, s2 *NewShardData) bool {
	return s1.startTime.Unix() == s2.StartTime.Unix() &&
		s1.endTime.Unix() == s2.EndTime.Unix() &&
		s1.SpaceName == s2.SpaceName &&
		s1.Database == s2.Database
}

// Add shards expects all shards to be of the same type (long term or short term) and have the same
// start and end times. This is called to add the shard set for a given duration. If existing
// shards have the same times, those are returned.
func (self *ClusterConfiguration) AddShards(shards []*NewShardData) ([]*ShardData, error) {
	self.shardLock.Lock()
	defer self.shardLock.Unlock()

	if len(shards) == 0 {
		return nil, errors.New("AddShards called without shards")
	}
	database := shards[0].Database
	spaceName := shards[0].SpaceName

	// first check if there are shards that match this time. If so, return those.
	createdShards := make([]*ShardData, 0)

	for _, s := range self.shardsById {
		if areShardsEqual(s, shards[0]) {
			createdShards = append(createdShards, s)
		}
	}

	if len(createdShards) > 0 {
		log.Debug("AddShards called when shards already existing")
		return createdShards, nil
	}

	for _, newShard := range shards {
		id := self.lastShardIdUsed + 1
		self.lastShardIdUsed = id
		shard := NewShard(id, newShard.StartTime, newShard.EndTime, database, spaceName, self.wal)
		servers := make([]*ClusterServer, 0)
		for _, serverId := range newShard.ServerIds {
			// if a shard is created before the local server then the local
			// server can't be one of the servers the shard belongs to,
			// since the shard was created before the server existed
			if self.LocalServer != nil && serverId == self.LocalServer.Id {
				err := shard.SetLocalStore(self.shardStore, self.LocalServer.Id)
				if err != nil {
					log.Error("AddShards: error setting local store: ", err)
					return nil, err
				}
			} else {
				servers = append(servers, self.GetServerById(&serverId))
			}
		}
		shard.SetServers(servers)

		createdShards = append(createdShards, shard)

		log.Info("Adding shard to %s: %d - start: %s (%d). end: %s (%d). isLocal: %v. servers: %v",
			shard.SpaceName, shard.Id(),
			shard.StartTime().Format("Mon Jan 2 15:04:05 -0700 MST 2006"), shard.StartTime().Unix(),
			shard.EndTime().Format("Mon Jan 2 15:04:05 -0700 MST 2006"), shard.EndTime().Unix(),
			shard.IsLocal, shard.ServerIds())
	}

	// now add the created shards to the indexes and all that
	space := self.getShardSpaceByDatabaseAndName(database, spaceName)
	if space == nil {
		return nil, fmt.Errorf("Unable to find shard space %s for database %s", spaceName, database)
	}
	space.shards = append(space.shards, createdShards...)
	SortShardsByTimeDescending(space.shards)

	for _, s := range createdShards {
		self.shardsById[s.id] = s
	}

	return createdShards, nil
}

func (self *ClusterConfiguration) MarshalNewShardArrayToShards(newShards []*NewShardData) ([]*ShardData, error) {
	shards := make([]*ShardData, len(newShards))
	for i, s := range newShards {
		shard := NewShard(s.Id, s.StartTime, s.EndTime, s.Database, s.SpaceName, self.wal)
		servers := make([]*ClusterServer, 0)
		for _, serverId := range s.ServerIds {
			if serverId == self.LocalServer.Id {
				err := shard.SetLocalStore(self.shardStore, self.LocalServer.Id)
				if err != nil {
					log.Error("AddShards: error setting local store: ", err)
					return nil, err
				}
			} else {
				servers = append(servers, self.GetServerById(&serverId))
			}
		}
		shard.SetServers(servers)
		shards[i] = shard
	}
	return shards, nil
}

// This function is for the request handler to get the shard to write a
// request to locally.
func (self *ClusterConfiguration) GetLocalShardById(id uint32) *ShardData {
	self.shardLock.RLock()
	defer self.shardLock.RUnlock()
	shard := self.shardsById[id]

	// If it's nil it just means that it hasn't been replicated by Raft yet.
	// Just create a fake local shard temporarily for the write.
	if shard == nil {
		shard = NewShard(id, time.Now(), time.Now(), "", "", self.wal)
		shard.SetServers([]*ClusterServer{})
		shard.SetLocalStore(self.shardStore, self.LocalServer.Id)
	}
	return shard
}

func (self *ClusterConfiguration) DropShard(shardId uint32, serverIds []uint32) error {
	// take it out of the memory map so writes and queries stop going to it
	self.updateOrRemoveShard(shardId, serverIds)

	// now actually remove it from disk if it lives here
	for _, serverId := range serverIds {
		if serverId == self.LocalServer.Id {
			self.shardStore.DeleteShard(shardId)
			return nil
		}
	}
	return nil
}

func (self *ClusterConfiguration) DropSeries(database, series string) error {
	fields, err := self.MetaStore.DropSeries(database, series)
	if err != nil {
		return err
	}
	go func() {
		for _, s := range self.shardsById {
			s.DropFields(fields)
		}
	}()
	return nil
}

func (self *ClusterConfiguration) updateOrRemoveShard(shardId uint32, serverIds []uint32) {
	self.shardLock.Lock()
	defer self.shardLock.Unlock()
	shard := self.shardsById[shardId]

	if shard == nil {
		log.Error("Attempted to remove shard %d, which we couldn't find. %d shards currently loaded.", shardId, len(self.shardsById))
		return
	}

	// we're removing the shard from the entire cluster
	if len(shard.serverIds) == len(serverIds) {
		self.removeShard(shard)
		return
	}

	// just remove the shard from the specified servers
	newIds := make([]uint32, 0)
	for _, oldId := range shard.serverIds {
		include := true
		for _, removeId := range serverIds {
			if oldId == removeId {
				include = false
				break
			}
		}
		if include {
			newIds = append(newIds, oldId)
		}
	}
	shard.serverIds = newIds
}

func (self *ClusterConfiguration) removeShard(shard *ShardData) {
	delete(self.shardsById, shard.id)

	// now remove it from the database shard spaces
	space := self.getShardSpaceByDatabaseAndName(shard.Database, shard.SpaceName)
	spaceShards := make([]*ShardData, 0)
	for _, s := range space.shards {
		if s.id != shard.id {
			spaceShards = append(spaceShards, s)
		}
	}
	space.shards = spaceShards
	return
}
