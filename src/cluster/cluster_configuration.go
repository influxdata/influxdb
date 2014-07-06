package cluster

import (
	"bytes"
	"common"
	"configuration"
	"crypto/sha1"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"parser"
	"protocol"
	"sort"
	"sync"
	"time"
	"wal"

	log "code.google.com/p/log4go"
)

// defined by cluster config (in cluster package)
type QuerySpec interface {
	GetStartTime() time.Time
	GetEndTime() time.Time
	Database() string
	TableNames() []string
	GetGroupByInterval() *time.Duration
	AllShardsQuery() bool
	IsRegex() bool
	ShouldQueryShortTermAndLongTerm() (shouldQueryShortTerm bool, shouldQueryLongTerm bool)
}

type WAL interface {
	AssignSequenceNumbersAndLog(request *protocol.Request, shard wal.Shard) (uint32, error)
	Commit(requestNumber uint32, serverId uint32) error
	CreateCheckpoint() error
	RecoverServerFromRequestNumber(requestNumber uint32, shardIds []uint32, yield func(request *protocol.Request, shardId uint32) error) error
	RecoverServerFromLastCommit(serverId uint32, shardIds []uint32, yield func(request *protocol.Request, shardId uint32) error) error
}

type ShardCreator interface {
	// the shard creator expects all shards to be of the same type (long term or short term) and have the same
	// start and end times. This is called to create the shard set for a given duration.
	CreateShards(shards []*NewShardData) ([]*ShardData, error)
}

const (
	FIRST_LOWER_CASE_CHARACTER = uint8('a')
)

/*
  This struct stores all the metadata confiugration information about a running cluster. This includes
  the servers in the cluster and their state, databases, users, and which continuous queries are running.
*/
type ClusterConfiguration struct {
	createDatabaseLock         sync.RWMutex
	DatabaseReplicationFactors map[string]struct{}
	usersLock                  sync.RWMutex
	clusterAdmins              map[string]*ClusterAdmin
	dbUsers                    map[string]map[string]*DbUser
	servers                    []*ClusterServer
	serversLock                sync.RWMutex
	continuousQueries          map[string][]*ContinuousQuery
	continuousQueriesLock      sync.RWMutex
	ParsedContinuousQueries    map[string]map[uint32]*parser.SelectQuery
	continuousQueryTimestamp   time.Time
	LocalServer                *ClusterServer
	config                     *configuration.Configuration
	addedLocalServerWait       chan bool
	addedLocalServer           bool
	connectionCreator          func(string) ServerConnection
	shardStore                 LocalShardStore
	wal                        WAL
	longTermShards             []*ShardData
	shortTermShards            []*ShardData
	lastShardIdUsed            uint32
	random                     *rand.Rand
	lastServerToGetShard       *ClusterServer
	shardCreator               ShardCreator
	shardLock                  sync.Mutex
	shardsById                 map[uint32]*ShardData
	shardsByIdLock             sync.RWMutex
	LocalRaftName              string
	writeBuffers               []*WriteBuffer
}

type ContinuousQuery struct {
	Id    uint32
	Query string
}

type Database struct {
	Name string `json:"name"`
}

func NewClusterConfiguration(
	config *configuration.Configuration,
	wal WAL,
	shardStore LocalShardStore,
	connectionCreator func(string) ServerConnection) *ClusterConfiguration {
	return &ClusterConfiguration{
		DatabaseReplicationFactors: make(map[string]struct{}),
		clusterAdmins:              make(map[string]*ClusterAdmin),
		dbUsers:                    make(map[string]map[string]*DbUser),
		continuousQueries:          make(map[string][]*ContinuousQuery),
		ParsedContinuousQueries:    make(map[string]map[uint32]*parser.SelectQuery),
		servers:                    make([]*ClusterServer, 0),
		config:                     config,
		addedLocalServerWait:       make(chan bool, 1),
		connectionCreator:          connectionCreator,
		shardStore:                 shardStore,
		wal:                        wal,
		longTermShards:             make([]*ShardData, 0),
		shortTermShards:            make([]*ShardData, 0),
		random:                     rand.New(rand.NewSource(time.Now().UnixNano())),
		shardsById:                 make(map[uint32]*ShardData, 0),
	}
}

func (self *ClusterConfiguration) SetShardCreator(shardCreator ShardCreator) {
	self.shardCreator = shardCreator
}

// called by the server, this will wake up every 10 mintues to see if it should
// create a shard for the next window of time. This way shards get created before
// a bunch of writes stream in and try to create it all at the same time.
func (self *ClusterConfiguration) CreateFutureShardsAutomaticallyBeforeTimeComes() {
	go func() {
		for {
			time.Sleep(time.Minute * 10)
			log.Debug("Checking to see if future shards should be created")
			self.automaticallyCreateFutureShard(self.shortTermShards, SHORT_TERM)
			self.automaticallyCreateFutureShard(self.longTermShards, LONG_TERM)
		}
	}()
}

func (self *ClusterConfiguration) automaticallyCreateFutureShard(shards []*ShardData, shardType ShardType) {
	if len(shards) == 0 {
		// don't automatically create shards if they haven't created any yet.
		return
	}
	latestShard := shards[0]
	if latestShard.endTime.Add(-15*time.Minute).Unix() < time.Now().Unix() {
		newShardTime := latestShard.endTime.Add(time.Second)
		microSecondEpochForNewShard := newShardTime.Unix() * 1000 * 1000
		log.Info("Automatically creating shard for %s", newShardTime.Format("Mon Jan 2 15:04:05 -0700 MST 2006"))
		self.createShards(microSecondEpochForNewShard, shardType)
	}
}

func (self *ClusterConfiguration) ServerId() uint32 {
	return self.LocalServer.Id
}

func (self *ClusterConfiguration) IsSingleServer() bool {
	return len(self.servers) < 2
}

func (self *ClusterConfiguration) Servers() []*ClusterServer {
	return self.servers
}

// This function will wait until the configuration has received an addPotentialServer command for
// this local server.
func (self *ClusterConfiguration) WaitForLocalServerLoaded() {
	// It's possible during initialization if Raft hasn't finished relpaying the log file or joining
	// the cluster that the cluster config won't have any servers. Wait for a little bit and retry, but error out eventually.
	<-self.addedLocalServerWait
}

func (self *ClusterConfiguration) GetServerByRaftName(name string) *ClusterServer {
	for _, server := range self.servers {
		if server.RaftName == name {
			return server
		}
	}
	return nil
}

func (self *ClusterConfiguration) GetServerById(id *uint32) *ClusterServer {
	for _, server := range self.servers {
		if server.Id == *id {
			return server
		}
	}
	log.Warn("Couldn't find server with id %d. Cluster servers: %#v", *id, self.servers)
	return nil
}

func (self *ClusterConfiguration) GetServerByProtobufConnectionString(connectionString string) *ClusterServer {
	for _, server := range self.servers {
		if server.ProtobufConnectionString == connectionString {
			return server
		}
	}
	return nil
}

// Return per shard request numbers for the local server and all remote servers
func (self *ClusterConfiguration) HasUncommitedWrites() bool {
	for _, buffer := range self.writeBuffers {
		if buffer.HasUncommitedWrites() {
			return true
		}
	}
	return false
}

func (self *ClusterConfiguration) ChangeProtobufConnectionString(server *ClusterServer) {
	if server.connection != nil {
		server.connection.Close()
	}
	server.connection = self.connectionCreator(server.ProtobufConnectionString)
	server.Connect()
}

func (self *ClusterConfiguration) RemoveServer(server *ClusterServer) error {
	server.connection.Close()
	i := 0
	l := len(self.servers)
	for i = 0; i < l; i++ {
		if self.servers[i].Id == server.Id {
			log.Debug("Found server %d at index %d", server.Id, i)
			break
		}
	}
	if i == l {
		return fmt.Errorf("Cannot find server %d", server.Id)
	}
	self.servers[i], self.servers = self.servers[l-1], self.servers[:l-1]
	log.Debug("Removed server %d", server.Id)
	return nil
}

func (self *ClusterConfiguration) AddPotentialServer(server *ClusterServer) {
	self.serversLock.Lock()
	defer self.serversLock.Unlock()
	server.State = Potential
	self.servers = append(self.servers, server)
	server.Id = uint32(len(self.servers))
	log.Info("Added server to cluster config: %d, %s, %s", server.Id, server.RaftConnectionString, server.ProtobufConnectionString)
	log.Info("Checking whether this is the local server local: %s, new: %s", self.config.ProtobufConnectionString(), server.ProtobufConnectionString)

	if server.RaftName == self.LocalRaftName && self.addedLocalServer {
		panic("how did we add the same server twice ?")
	}

	// if this is the local server unblock WaitForLocalServerLoaded()
	// and set the local connection string and id
	if server.RaftName == self.LocalRaftName {
		log.Info("Added the local server")
		self.LocalServer = server
		self.addedLocalServerWait <- true
		self.addedLocalServer = true
		return
	}

	// if this isn't the local server, connect to it
	log.Info("Connecting to ProtobufServer: %s from %s", server.ProtobufConnectionString, self.config.ProtobufConnectionString())
	if server.connection == nil {
		server.connection = self.connectionCreator(server.ProtobufConnectionString)
		server.Connect()
	}
	writeBuffer := NewWriteBuffer(fmt.Sprintf("%d", server.GetId()), server, self.wal, server.Id, self.config.PerServerWriteBufferSize)
	self.writeBuffers = append(self.writeBuffers, writeBuffer)
	server.SetWriteBuffer(writeBuffer)
	server.StartHeartbeat()
	return
}

func (self *ClusterConfiguration) DatabasesExists(db string) bool {
	self.createDatabaseLock.RLock()
	defer self.createDatabaseLock.RUnlock()

	_, ok := self.DatabaseReplicationFactors[db]
	return ok
}

func (self *ClusterConfiguration) GetDatabases() []*Database {
	self.createDatabaseLock.RLock()
	defer self.createDatabaseLock.RUnlock()

	dbs := make([]*Database, 0, len(self.DatabaseReplicationFactors))
	for name, _ := range self.DatabaseReplicationFactors {
		dbs = append(dbs, &Database{Name: name})
	}
	return dbs
}

func (self *ClusterConfiguration) DatabaseExists(name string) bool {
	if _, ok := self.DatabaseReplicationFactors[name]; ok {
		return true
	} else {
		return false
	}
}

func (self *ClusterConfiguration) CreateDatabase(name string) error {
	self.createDatabaseLock.Lock()
	defer self.createDatabaseLock.Unlock()

	if _, ok := self.DatabaseReplicationFactors[name]; ok {
		return common.NewDatabaseExistsError(name)
	}
	self.DatabaseReplicationFactors[name] = struct{}{}
	return nil
}

func (self *ClusterConfiguration) DropDatabase(name string) error {
	self.createDatabaseLock.Lock()
	defer self.createDatabaseLock.Unlock()

	if _, ok := self.DatabaseReplicationFactors[name]; !ok {
		return fmt.Errorf("Database %s doesn't exist", name)
	}

	delete(self.DatabaseReplicationFactors, name)

	self.continuousQueriesLock.Lock()
	defer self.continuousQueriesLock.Unlock()
	delete(self.continuousQueries, name)
	delete(self.ParsedContinuousQueries, name)

	self.usersLock.Lock()
	defer self.usersLock.Unlock()

	delete(self.dbUsers, name)
	return nil
}

func (self *ClusterConfiguration) CreateContinuousQuery(db string, query string) error {
	self.continuousQueriesLock.Lock()
	defer self.continuousQueriesLock.Unlock()

	maxId := uint32(0)
	for _, query := range self.continuousQueries[db] {
		if query.Id > maxId {
			maxId = query.Id
		}
	}

	return self.addContinuousQuery(db, &ContinuousQuery{maxId + 1, query})
}

func (self *ClusterConfiguration) addContinuousQuery(db string, query *ContinuousQuery) error {
	if self.continuousQueries == nil {
		self.continuousQueries = map[string][]*ContinuousQuery{}
	}

	if self.ParsedContinuousQueries == nil {
		self.ParsedContinuousQueries = map[string]map[uint32]*parser.SelectQuery{}
	}

	selectQuery, err := parser.ParseSelectQuery(query.Query)
	if err != nil {
		return fmt.Errorf("Failed to parse continuous query: %s", query)
	}

	if self.ParsedContinuousQueries[db] == nil {
		self.ParsedContinuousQueries[db] = map[uint32]*parser.SelectQuery{query.Id: selectQuery}
	} else {
		self.ParsedContinuousQueries[db][query.Id] = selectQuery
	}
	self.continuousQueries[db] = append(self.continuousQueries[db], query)
	return nil
}

func (self *ClusterConfiguration) SetContinuousQueryTimestamp(timestamp time.Time) error {
	self.continuousQueriesLock.Lock()
	defer self.continuousQueriesLock.Unlock()

	self.continuousQueryTimestamp = timestamp

	return nil
}

func (self *ClusterConfiguration) DeleteContinuousQuery(db string, id uint32) error {
	self.continuousQueriesLock.Lock()
	defer self.continuousQueriesLock.Unlock()

	for i, query := range self.continuousQueries[db] {
		if query.Id == id {
			q := self.continuousQueries[db]
			q[len(q)-1], q[i], q = nil, q[len(q)-1], q[:len(q)-1]
			self.continuousQueries[db] = q
			delete(self.ParsedContinuousQueries[db], id)
			break
		}
	}

	return nil
}

func (self *ClusterConfiguration) GetContinuousQueries(db string) []*ContinuousQuery {
	self.continuousQueriesLock.Lock()
	defer self.continuousQueriesLock.Unlock()

	return self.continuousQueries[db]
}

func (self *ClusterConfiguration) GetLocalConfiguration() *configuration.Configuration {
	return self.config
}

func (self *ClusterConfiguration) GetDbUsers(db string) []common.User {
	self.usersLock.RLock()
	defer self.usersLock.RUnlock()

	dbUsers := self.dbUsers[db]
	users := make([]common.User, 0, len(dbUsers))
	for name, _ := range dbUsers {
		dbUser := dbUsers[name]
		users = append(users, dbUser)
	}
	return users
}

func (self *ClusterConfiguration) GetDbUser(db, username string) *DbUser {
	self.usersLock.RLock()
	defer self.usersLock.RUnlock()

	dbUsers := self.dbUsers[db]
	if dbUsers == nil {
		return nil
	}
	return dbUsers[username]
}

func (self *ClusterConfiguration) SaveDbUser(u *DbUser) {
	self.usersLock.Lock()
	defer self.usersLock.Unlock()
	db := u.GetDb()
	dbUsers := self.dbUsers[db]
	if u.IsDeleted() {
		if dbUsers == nil {
			return
		}
		delete(dbUsers, u.GetName())
		return
	}
	if dbUsers == nil {
		dbUsers = map[string]*DbUser{}
		self.dbUsers[db] = dbUsers
	}
	dbUsers[u.GetName()] = u
}

func (self *ClusterConfiguration) ChangeDbUserPassword(db, username, hash string) error {
	self.usersLock.Lock()
	defer self.usersLock.Unlock()
	dbUsers := self.dbUsers[db]
	if dbUsers == nil {
		return fmt.Errorf("Invalid database name %s", db)
	}
	if dbUsers[username] == nil {
		return fmt.Errorf("Invalid username %s", username)
	}
	dbUsers[username].ChangePassword(hash)
	return nil
}

func (self *ClusterConfiguration) ChangeDbUserPermissions(db, username, readPermissions, writePermissions string) error {
	self.usersLock.Lock()
	defer self.usersLock.Unlock()
	dbUsers := self.dbUsers[db]
	if dbUsers == nil {
		return fmt.Errorf("Invalid database name %s", db)
	}
	if dbUsers[username] == nil {
		return fmt.Errorf("Invalid username %s", username)
	}
	dbUsers[username].ChangePermissions(readPermissions, writePermissions)
	return nil
}

func (self *ClusterConfiguration) GetClusterAdmins() (names []string) {
	self.usersLock.RLock()
	defer self.usersLock.RUnlock()

	clusterAdmins := self.clusterAdmins
	for name, _ := range clusterAdmins {
		names = append(names, name)
	}
	return
}

func (self *ClusterConfiguration) GetClusterAdmin(username string) *ClusterAdmin {
	self.usersLock.RLock()
	defer self.usersLock.RUnlock()

	return self.clusterAdmins[username]
}

func (self *ClusterConfiguration) SaveClusterAdmin(u *ClusterAdmin) {
	self.usersLock.Lock()
	defer self.usersLock.Unlock()
	if u.IsDeleted() {
		delete(self.clusterAdmins, u.GetName())
		return
	}
	self.clusterAdmins[u.GetName()] = u
	u.ChangePassword(u.Hash)
}

type SavedConfiguration struct {
	Databases         map[string]uint8
	Admins            map[string]*ClusterAdmin
	DbUsers           map[string]map[string]*DbUser
	Servers           []*ClusterServer
	ShortTermShards   []*NewShardData
	LongTermShards    []*NewShardData
	ContinuousQueries map[string][]*ContinuousQuery
	LastShardIdUsed   uint32
}

func (self *ClusterConfiguration) Save() ([]byte, error) {
	log.Debug("Dumping the cluster configuration")
	data := &SavedConfiguration{
		Databases:         make(map[string]uint8, len(self.DatabaseReplicationFactors)),
		Admins:            self.clusterAdmins,
		DbUsers:           self.dbUsers,
		Servers:           self.servers,
		ContinuousQueries: self.continuousQueries,
		ShortTermShards:   self.convertShardsToNewShardData(self.shortTermShards),
		LongTermShards:    self.convertShardsToNewShardData(self.longTermShards),
		LastShardIdUsed:   self.lastShardIdUsed,
	}

	for k, _ := range self.DatabaseReplicationFactors {
		data.Databases[k] = 0
	}

	b := bytes.NewBuffer(nil)
	err := gob.NewEncoder(b).Encode(&data)
	if err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

func (self *ClusterConfiguration) convertShardsToNewShardData(shards []*ShardData) []*NewShardData {
	newShardData := make([]*NewShardData, len(shards), len(shards))
	for i, shard := range shards {
		newShardData[i] = &NewShardData{Id: shard.id, Type: shard.shardType, StartTime: shard.startTime, EndTime: shard.endTime, ServerIds: shard.serverIds, DurationSplit: shard.durationIsSplit}
	}
	return newShardData
}

func (self *ClusterConfiguration) convertNewShardDataToShards(newShards []*NewShardData) []*ShardData {
	shards := make([]*ShardData, len(newShards), len(newShards))
	for i, newShard := range newShards {
		shard := NewShard(newShard.Id, newShard.StartTime, newShard.EndTime, newShard.Type, newShard.DurationSplit, self.wal)
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

func (self *ClusterConfiguration) Recovery(b []byte) error {
	log.Info("Recovering the cluster configuration")
	data := &SavedConfiguration{}

	err := gob.NewDecoder(bytes.NewReader(b)).Decode(&data)
	if err != nil {
		log.Error("Error while decoding snapshot: %s", err)
		return err
	}

	self.DatabaseReplicationFactors = make(map[string]struct{}, len(data.Databases))
	for k, _ := range data.Databases {
		self.DatabaseReplicationFactors[k] = struct{}{}
	}
	self.clusterAdmins = data.Admins
	self.dbUsers = data.DbUsers
	self.servers = data.Servers

	for _, server := range self.servers {
		log.Info("Checking whether %s is the local server %s", server.RaftName, self.LocalRaftName)
		if server.RaftName == self.LocalRaftName {
			self.LocalServer = server
			self.addedLocalServerWait <- true
			self.addedLocalServer = true
			continue
		}

		server.connection = self.connectionCreator(server.ProtobufConnectionString)
		writeBuffer := NewWriteBuffer(fmt.Sprintf("server: %d", server.GetId()), server, self.wal, server.Id, self.config.PerServerWriteBufferSize)
		self.writeBuffers = append(self.writeBuffers, writeBuffer)
		server.SetWriteBuffer(writeBuffer)
		server.Connect()
		server.StartHeartbeat()
	}

	self.shardsByIdLock.Lock()
	self.shardLock.Lock()
	defer self.shardsByIdLock.Unlock()
	defer self.shardLock.Unlock()
	self.shortTermShards = self.convertNewShardDataToShards(data.ShortTermShards)
	self.longTermShards = self.convertNewShardDataToShards(data.LongTermShards)

	highestShardId := uint32(0)
	for _, s := range self.shortTermShards {
		shard := s
		self.shardsById[s.id] = shard
		if s.id > highestShardId {
			highestShardId = s.id
		}
	}
	for _, s := range self.longTermShards {
		shard := s
		self.shardsById[s.id] = shard
		if s.id > highestShardId {
			highestShardId = s.id
		}
	}

	if data.LastShardIdUsed == 0 {
		self.lastShardIdUsed = highestShardId
	} else {
		self.lastShardIdUsed = data.LastShardIdUsed
	}

	for db, queries := range data.ContinuousQueries {
		for _, query := range queries {
			self.addContinuousQuery(db, query)
		}
	}

	return nil
}

func (self *ClusterConfiguration) AuthenticateDbUser(db, username, password string) (common.User, error) {
	dbUsers := self.dbUsers[db]
	if dbUsers == nil || dbUsers[username] == nil {
		return nil, common.NewAuthorizationError("Invalid username/password")
	}
	user := dbUsers[username]
	if user.isValidPwd(password) {
		return user, nil
	}
	return nil, common.NewAuthorizationError("Invalid username/password")
}

func (self *ClusterConfiguration) AuthenticateClusterAdmin(username, password string) (common.User, error) {
	user := self.clusterAdmins[username]
	if user == nil {
		return nil, common.NewAuthorizationError("Invalid username/password")
	}
	if user.isValidPwd(password) {
		return user, nil
	}
	return nil, common.NewAuthorizationError("Invalid username/password")
}

func (self *ClusterConfiguration) HasContinuousQueries() bool {
	return self.continuousQueries != nil && len(self.continuousQueries) > 0
}

func (self *ClusterConfiguration) LastContinuousQueryRunTime() time.Time {
	return self.continuousQueryTimestamp
}

func (self *ClusterConfiguration) SetLastContinuousQueryRunTime(t time.Time) {
	self.continuousQueryTimestamp = t
}

func (self *ClusterConfiguration) GetMapForJsonSerialization() map[string]interface{} {
	jsonObject := make(map[string]interface{})
	dbs := make([]string, 0)
	for db, _ := range self.DatabaseReplicationFactors {
		dbs = append(dbs, db)
	}
	jsonObject["databases"] = dbs
	jsonObject["cluster_admins"] = self.clusterAdmins
	jsonObject["database_users"] = self.dbUsers
	return jsonObject
}

func (self *ClusterConfiguration) GetShardToWriteToBySeriesAndTime(db, series string, microsecondsEpoch int64) (*ShardData, error) {
	shards := self.shortTermShards
	//	split := self.config.ShortTermShard.Split
	hasRandomSplit := self.config.ShortTermShard.HasRandomSplit()
	splitRegex := self.config.ShortTermShard.SplitRegex()
	shardType := SHORT_TERM

	firstChar := series[0]
	if firstChar < FIRST_LOWER_CASE_CHARACTER {
		shardType = LONG_TERM
		shards = self.longTermShards
		//		split = self.config.LongTermShard.Split
		hasRandomSplit = self.config.LongTermShard.HasRandomSplit()
		splitRegex = self.config.LongTermShard.SplitRegex()
	}
	matchingShards := make([]*ShardData, 0)
	for _, s := range shards {
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
		matchingShards, err = self.createShards(microsecondsEpoch, shardType)
		if err != nil {
			return nil, err
		}
	}

	if len(matchingShards) == 1 {
		return matchingShards[0], nil
	}

	if hasRandomSplit && splitRegex.MatchString(series) {
		return matchingShards[self.random.Intn(len(matchingShards))], nil
	}
	index := HashDbAndSeriesToInt(db, series)
	index = index % len(matchingShards)
	return matchingShards[index], nil
}

func (self *ClusterConfiguration) createShards(microsecondsEpoch int64, shardType ShardType) ([]*ShardData, error) {
	numberOfShardsToCreateForDuration := 1
	var secondsOfDuration float64
	if shardType == LONG_TERM {
		numberOfShardsToCreateForDuration = self.config.LongTermShard.Split
		secondsOfDuration = self.config.LongTermShard.ParsedDuration().Seconds()
	} else {
		numberOfShardsToCreateForDuration = self.config.ShortTermShard.Split
		secondsOfDuration = self.config.ShortTermShard.ParsedDuration().Seconds()
	}
	startIndex := 0
	if self.lastServerToGetShard != nil {
		for i, server := range self.servers {
			if server == self.lastServerToGetShard {
				startIndex = i + 1
			}
		}
	}

	shards := make([]*NewShardData, 0)
	startTime, endTime := self.getStartAndEndBasedOnDuration(microsecondsEpoch, secondsOfDuration)

	log.Info("createShards: start: %s. end: %s",
		startTime.Format("Mon Jan 2 15:04:05 -0700 MST 2006"), endTime.Format("Mon Jan 2 15:04:05 -0700 MST 2006"))

	for i := numberOfShardsToCreateForDuration; i > 0; i-- {
		serverIds := make([]uint32, 0)

		// if they have the replication factor set higher than the number of servers in the cluster, limit it
		rf := self.config.ReplicationFactor
		if rf > len(self.servers) {
			rf = len(self.servers)
		}

		for rf = rf; rf > 0; rf-- {
			if startIndex >= len(self.servers) {
				startIndex = 0
			}
			server := self.servers[startIndex]
			self.lastServerToGetShard = server
			serverIds = append(serverIds, server.Id)
			startIndex += 1
		}
		shards = append(shards, &NewShardData{StartTime: *startTime, EndTime: *endTime, ServerIds: serverIds, Type: shardType})
	}

	// call out to rafter server to create the shards (or return shard objects that the leader already knows about)
	createdShards, err := self.shardCreator.CreateShards(shards)
	if err != nil {
		return nil, err
	}
	return createdShards, nil
}

func (self *ClusterConfiguration) CreateCheckpoint() error {
	return self.wal.CreateCheckpoint()
}

func (self *ClusterConfiguration) getStartAndEndBasedOnDuration(microsecondsEpoch int64, duration float64) (*time.Time, *time.Time) {
	startTimeSeconds := math.Floor(float64(microsecondsEpoch)/1000.0/1000.0/duration) * duration
	startTime := time.Unix(int64(startTimeSeconds), 0)
	endTime := time.Unix(int64(startTimeSeconds+duration), 0)

	return &startTime, &endTime
}

func (self *ClusterConfiguration) GetShards(querySpec *parser.QuerySpec) []*ShardData {
	self.shardsByIdLock.RLock()
	defer self.shardsByIdLock.RUnlock()

	shouldQueryShortTerm, shouldQueryLongTerm := querySpec.ShouldQueryShortTermAndLongTerm()

	if shouldQueryLongTerm && shouldQueryShortTerm {
		shards := make([]*ShardData, 0)
		shards = append(shards, self.getShardRange(querySpec, self.shortTermShards)...)
		shards = append(shards, self.getShardRange(querySpec, self.longTermShards)...)
		if querySpec.IsAscending() {
			SortShardsByTimeAscending(shards)
		} else {
			SortShardsByTimeDescending(shards)
		}
		return shards
	}

	var shards []*ShardData
	if shouldQueryLongTerm {
		shards = self.getShardRange(querySpec, self.longTermShards)
	} else {
		shards = self.getShardRange(querySpec, self.shortTermShards)
	}
	if querySpec.IsAscending() {
		newShards := append([]*ShardData{}, shards...)
		SortShardsByTimeAscending(newShards)
		return newShards
	}
	return shards
}

func (self *ClusterConfiguration) GetLongTermShards() []*ShardData {
	return self.longTermShards
}

func (self *ClusterConfiguration) GetShortTermShards() []*ShardData {
	return self.shortTermShards
}

func (self *ClusterConfiguration) GetAllShards() []*ShardData {
	sh := append([]*ShardData{}, self.shortTermShards...)
	return append(sh, self.longTermShards...)
}

func (self *ClusterConfiguration) GetShard(id uint32) *ShardData {
	self.shardsByIdLock.RLock()
	shard := self.shardsById[id]
	self.shardsByIdLock.RUnlock()

	// may not be in the map, try to get it from the list
	if shard == nil {
		for _, s := range self.GetAllShards() {
			if s.id == id {
				shard = s
				break
			}
		}
	}
	return shard
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

// Add shards expects all shards to be of the same type (long term or short term) and have the same
// start and end times. This is called to add the shard set for a given duration. If existing
// shards have the same times, those are returned.
func (self *ClusterConfiguration) AddShards(shards []*NewShardData) ([]*ShardData, error) {
	self.shardLock.Lock()
	defer self.shardLock.Unlock()

	if len(shards) == 0 {
		return nil, errors.New("AddShards called without shards")
	}

	// first check if there are shards that match this time. If so, return those.
	createdShards := make([]*ShardData, 0)

	startTime := shards[0].StartTime
	endTime := shards[0].EndTime

	shardType := SHORT_TERM
	existingShards := self.shortTermShards
	if shards[0].Type == LONG_TERM {
		shardType = LONG_TERM
		existingShards = self.longTermShards
	}

	for _, s := range existingShards {
		if s.startTime.Unix() == startTime.Unix() && s.endTime.Unix() == endTime.Unix() {
			createdShards = append(createdShards, s)
		}
	}

	if len(createdShards) > 0 {
		log.Info("AddShards called when shards already existing")
		return createdShards, nil
	}

	durationIsSplit := len(shards) > 1
	for _, newShard := range shards {
		id := self.lastShardIdUsed + 1
		self.lastShardIdUsed = id
		shard := NewShard(id, newShard.StartTime, newShard.EndTime, shardType, durationIsSplit, self.wal)
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

		self.shardsByIdLock.Lock()
		self.shardsById[shard.id] = shard
		self.shardsByIdLock.Unlock()

		message := "Adding long term shard"
		if newShard.Type == LONG_TERM {
			self.longTermShards = append(self.longTermShards, shard)
			SortShardsByTimeDescending(self.longTermShards)
		} else {
			message = "Adding short term shard"
			self.shortTermShards = append(self.shortTermShards, shard)
			SortShardsByTimeDescending(self.shortTermShards)
		}

		createdShards = append(createdShards, shard)

		log.Info("%s: %d - start: %s (%d). end: %s (%d). isLocal: %v. servers: %v",
			message, shard.Id(),
			shard.StartTime().Format("Mon Jan 2 15:04:05 -0700 MST 2006"), shard.StartTime().Unix(),
			shard.EndTime().Format("Mon Jan 2 15:04:05 -0700 MST 2006"), shard.EndTime().Unix(),
			shard.IsLocal, shard.ServerIds())
	}
	return createdShards, nil
}

func (self *ClusterConfiguration) MarshalNewShardArrayToShards(newShards []*NewShardData) ([]*ShardData, error) {
	shards := make([]*ShardData, len(newShards), len(newShards))
	durationIsSplit := len(newShards) > 1
	for i, s := range newShards {
		shard := NewShard(s.Id, s.StartTime, s.EndTime, s.Type, durationIsSplit, self.wal)
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
	self.shardsByIdLock.RLock()
	defer self.shardsByIdLock.RUnlock()
	shard := self.shardsById[id]

	// If it's nil it just means that it hasn't been replicated by Raft yet.
	// Just create a fake local shard temporarily for the write.
	if shard == nil {
		shard = NewShard(id, time.Now(), time.Now(), LONG_TERM, false, self.wal)
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
			return self.shardStore.DeleteShard(shardId)
		}
	}
	return nil
}

func (self *ClusterConfiguration) RecoverFromWAL() error {
	writeBuffer := NewWriteBuffer("local", self.shardStore, self.wal, self.LocalServer.Id, self.config.LocalStoreWriteBufferSize)
	self.writeBuffers = append(self.writeBuffers, writeBuffer)
	self.shardStore.SetWriteBuffer(writeBuffer)
	var waitForAll sync.WaitGroup
	for _, _server := range self.servers {
		server := _server
		waitForAll.Add(1)
		if server.RaftName == self.LocalRaftName {
			self.LocalServer = server
			go func(serverId uint32) {
				log.Info("Recovering local server")
				self.recover(serverId, self.shardStore)
				log.Info("Recovered local server")
				waitForAll.Done()
			}(server.Id)
		} else {
			go func(serverId uint32) {
				if server.connection == nil {
					server.connection = self.connectionCreator(server.ProtobufConnectionString)
					server.Connect()
				}
				log.Info("Recovering remote server %d", serverId)
				self.recover(serverId, server)
				log.Info("Recovered remote server %d", serverId)
				waitForAll.Done()
			}(server.Id)
		}
	}
	log.Info("Waiting for servers to recover")
	waitForAll.Wait()
	return nil
}

func (self *ClusterConfiguration) recover(serverId uint32, writer Writer) error {
	shardIds := self.shardIdsForServerId(serverId)
	if len(shardIds) == 0 {
		log.Info("No shards to recover for %d", serverId)
		return nil
	}

	log.Debug("replaying wal for server %d and shardIds %#v", serverId, shardIds)
	return self.wal.RecoverServerFromLastCommit(serverId, shardIds, func(request *protocol.Request, shardId uint32) error {
		if request == nil {
			log.Error("Error on recover, the wal yielded a nil request")
			return nil
		}
		requestNumber := request.GetRequestNumber()
		log.Debug("Sending request %s for shard %d to server %d", request.GetDescription(), shardId, serverId)
		err := writer.Write(request)
		if err != nil {
			return err
		}
		log.Debug("Finished sending request %d to server %d", request.GetRequestNumber(), serverId)
		return self.wal.Commit(requestNumber, serverId)
	})
}

func (self *ClusterConfiguration) shardIdsForServerId(serverId uint32) []uint32 {
	shardIds := make([]uint32, 0)
	for _, shard := range self.GetAllShards() {
		for _, id := range shard.serverIds {
			if id == serverId {
				shardIds = append(shardIds, shard.Id())
				break
			}
		}
	}
	return shardIds
}

func (self *ClusterConfiguration) updateOrRemoveShard(shardId uint32, serverIds []uint32) {
	shard := self.GetShard(shardId)
	if shard == nil {
		log.Error("Attempted to remove shard %d, which we couldn't find. %d shards currently loaded.", shardId, len(self.GetAllShards()))
		return
	}

	if len(shard.serverIds) == len(serverIds) {
		self.removeShard(shardId)
		return
	}
	self.shardsByIdLock.Lock()
	defer self.shardsByIdLock.Unlock()
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

func (self *ClusterConfiguration) removeShard(shardId uint32) {
	self.shardLock.Lock()
	self.shardsByIdLock.Lock()
	defer self.shardLock.Unlock()
	defer self.shardsByIdLock.Unlock()
	delete(self.shardsById, shardId)

	for i, shard := range self.shortTermShards {
		if shard.id == shardId {
			copy(self.shortTermShards[i:], self.shortTermShards[i+1:])
			self.shortTermShards[len(self.shortTermShards)-1] = nil
			self.shortTermShards = self.shortTermShards[:len(self.shortTermShards)-1]
			return
		}
	}

	for i, shard := range self.longTermShards {
		if shard.id == shardId {
			copy(self.longTermShards[i:], self.longTermShards[i+1:])
			self.longTermShards[len(self.longTermShards)-1] = nil
			self.longTermShards = self.longTermShards[:len(self.longTermShards)-1]
			return
		}
	}
}
