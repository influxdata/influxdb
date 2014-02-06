package cluster

import (
	"bytes"
	log "code.google.com/p/log4go"
	"common"
	"configuration"
	"encoding/gob"
	"errors"
	"fmt"
	"parser"
	"protocol"
	"sync"
	"sync/atomic"
	"time"
	"wal"
)

// defined by cluster config (in cluster package)
type QuerySpec interface {
	GetStartTime() time.Time
	GetEndTime() time.Time
	Database() string
	TableNames() []string
	IsRegex() bool
}

type WAL interface {
	AssignSequenceNumbersAndLog(request *protocol.Request, shard wal.Shard, servers []wal.Server) (uint32, error)
	Commit(requestNumber uint32, server wal.Server) error
	RecoverFromLog(yield func(request *protocol.Request, shard wal.Shard, server wal.Server) error) error
	RecoverServerFromRequestNumber(requestNumber uint32, server wal.Server, yield func(request *protocol.Request, shard wal.Shard) error) error
}

/*
  This struct stores all the metadata confiugration information about a running cluster. This includes
  the servers in the cluster and their state, databases, users, and which continuous queries are running.

  ClusterVersion is a monotonically increasing int that keeps track of different server configurations.
  For example, when you spin up a cluster and start writing data, the version will be 1. If you expand the
  cluster the version will be bumped. Using this the cluster is able to run two versions simultaneously
  while the new servers are being brought online.
*/
type ClusterConfiguration struct {
	createDatabaseLock         sync.RWMutex
	DatabaseReplicationFactors map[string]uint8
	usersLock                  sync.RWMutex
	clusterAdmins              map[string]*ClusterAdmin
	dbUsers                    map[string]map[string]*DbUser
	servers                    []*ClusterServer
	serversLock                sync.RWMutex
	continuousQueries          map[string][]*ContinuousQuery
	continuousQueriesLock      sync.RWMutex
	ParsedContinuousQueries    map[string]map[uint32]*parser.SelectQuery
	continuousQueryTimestamp   time.Time
	hasRunningServers          bool
	LocalServerId              uint32
	ClusterVersion             uint32
	config                     *configuration.Configuration
	addedLocalServerWait       chan bool
	addedLocalServer           bool
	connectionCreator          func(string) ServerConnection
	shardStore                 LocalShardStore
	wal                        WAL
}

type ContinuousQuery struct {
	Id    uint32
	Query string
}

type Database struct {
	Name              string `json:"name"`
	ReplicationFactor uint8  `json:"replicationFactor"`
}

func NewClusterConfiguration(
	config *configuration.Configuration,
	wal WAL,
	shardStore LocalShardStore,
	connectionCreator func(string) ServerConnection) *ClusterConfiguration {
	return &ClusterConfiguration{
		DatabaseReplicationFactors: make(map[string]uint8),
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
	}
}

func (self *ClusterConfiguration) ServerId() uint32 {
	return self.LocalServerId
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

func (self *ClusterConfiguration) GetReplicationFactor(database *string) uint8 {
	return self.DatabaseReplicationFactors[*database]
}

func (self *ClusterConfiguration) IsActive() bool {
	return self.hasRunningServers
}

func (self *ClusterConfiguration) SetActive() {
	self.serversLock.Lock()
	defer self.serversLock.Unlock()
	for _, server := range self.servers {
		server.State = Running
	}
	atomic.AddUint32(&self.ClusterVersion, uint32(1))
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
		if server.id == *id {
			return server
		}
	}
	log.Warn("Couldn't find server with id: ", *id, self.servers)
	return nil
}

type ServerToQuery struct {
	Server              *ClusterServer
	RingLocationToQuery uint32
}

// This function will return an array of servers to query and the number of ring locations to return per server.
// Queries are issued to every nth server in the cluster where n is the replication factor. We need the local host id
// because we want to issue the query locally and the nth servers from there.
// Optimally, the number of servers in a cluster will be evenly divisible by the replication factors used. For example,
// if you have a cluster with databases with RFs of 1, 2, and 3: optimal cluster sizes would be 6, 12, 18, 24, 30, etc.
// If that's not the case, one or more servers will have to filter out data from other servers on the fly, which could
// be a little more expensive.
func (self *ClusterConfiguration) GetServersToMakeQueryTo(database *string) (servers []*ServerToQuery, replicationFactor uint32) {
	replicationFactor = uint32(self.GetReplicationFactor(database))
	replicationFactorInt := int(replicationFactor)
	index := 0
	for i, s := range self.servers {
		if s.id == self.LocalServerId {
			index = i % replicationFactorInt
			break
		}
	}
	servers = make([]*ServerToQuery, 0, len(self.servers)/replicationFactorInt)
	serverCount := len(self.servers)
	for ; index < serverCount; index += replicationFactorInt {
		server := self.servers[index]
		servers = append(servers, &ServerToQuery{server, replicationFactor})
	}
	// need to maybe add a server and set which ones filter their data
	if serverCount%replicationFactorInt != 0 {
		/*
				Here's what this looks like with a few different ring sizes and replication factors.

			  server indexes
				0 1 2 3 4

				0   2   4   6     - 0 only sends his
				  1   3   5       - add 0 and have 1 only send his

				0     3     6     - have 0 only send his and 4
				  1     4     7   - have 1 only send his and 0
				    2     5       - add 0 and have 2 send only his and 1


				server indexes
				0 1 2 3 4 5 6 7

				0     3     6     9     - have 0 only send his and 7
				  1     4     7     10  - have 1 only send his and 0
				    2     5     8       - add 0 and have 2 only send his and 1

						We see that there are the number of cases equal to the replication factor. The last case
						always has us adding the first server in the ring. Then we're determining how much data
						the local server should be ignoring.
		*/
		lastIndexAdded := index - replicationFactorInt
		if serverCount-lastIndexAdded == replicationFactorInt {
			servers[0].RingLocationToQuery = uint32(replicationFactorInt - 1)
			servers = append(servers, &ServerToQuery{self.servers[0], replicationFactor})
		} else {
			servers[0].RingLocationToQuery = uint32(replicationFactorInt - 1)
		}
	}
	return servers, replicationFactor
}

// This method returns a function that can be passed into the datastore's ExecuteQuery method. It will tell the datastore
// if each point is something that should be returned in the query based on its ring location and if the query calls for
// data from different replicas.
//
// Params:
// - database: the name of the database
// - countOfServersToInclude: the number of replicas that this server will return data for
//
// Returns a function that returns true if the point should be filtered out. Otherwise the point should be included
// in the yielded time series
func (self *ClusterConfiguration) GetRingFilterFunction(database string, countOfServersToInclude uint32) func(database, series *string, time *int64) bool {
	serversToInclude := make([]*ClusterServer, 0, countOfServersToInclude)
	countServers := int(countOfServersToInclude)
	for i, s := range self.servers {
		if s.id == self.LocalServerId {
			serversToInclude = append(serversToInclude, s)
			for j := i - 1; j >= 0 && len(serversToInclude) < countServers; j-- {
				serversToInclude = append(serversToInclude, self.servers[j])
			}
			if len(serversToInclude) < countServers {
				for j := len(self.servers) - 1; len(serversToInclude) < countServers; j-- {
					serversToInclude = append(serversToInclude, self.servers[j])
				}
			}
		}
	}
	f := func(database, series *string, time *int64) bool {
		location := common.RingLocation(database, series, time)
		server := self.servers[location%len(self.servers)]
		for _, s := range serversToInclude {
			if s.id == server.id {
				return false
			}
		}
		return true
	}
	return f
}

func (self *ClusterConfiguration) GetServerIndexByLocation(location *int) int {
	return *location % len(self.servers)
}

func (self *ClusterConfiguration) GetOwnerIdByLocation(location *int) *uint32 {
	return &self.servers[self.GetServerIndexByLocation(location)].id
}

func (self *ClusterConfiguration) GetServersByRingLocation(database *string, location *int) []*ClusterServer {
	index := self.GetServerIndexByLocation(location)
	_, replicas := self.GetServersByIndexAndReplicationFactor(database, &index)
	return replicas
}

// This function returns the replicas of the given server
func (self *ClusterConfiguration) GetReplicas(server *ClusterServer, database *string) (*ClusterServer, []*ClusterServer) {
	for index, s := range self.servers {
		if s.id == server.id {
			return self.GetServersByIndexAndReplicationFactor(database, &index)
		}
	}

	return nil, nil
}

// This function returns the server that owns the ring location and a set of servers that are replicas (which include the onwer)
func (self *ClusterConfiguration) GetServersByIndexAndReplicationFactor(database *string, index *int) (*ClusterServer, []*ClusterServer) {
	replicationFactor := int(self.GetReplicationFactor(database))
	serverCount := len(self.servers)
	owner := self.servers[*index]
	if replicationFactor >= serverCount {
		return owner, self.servers
	}
	owners := make([]*ClusterServer, 0, replicationFactor)
	ownerCount := 0
	for i := *index; i < serverCount && ownerCount < replicationFactor; i++ {
		owners = append(owners, self.servers[i])
		ownerCount++
	}
	for i := 0; ownerCount < replicationFactor; i++ {
		owners = append(owners, self.servers[i])
		ownerCount++
	}
	return owner, owners
}

func (self *ClusterConfiguration) GetServerByProtobufConnectionString(connectionString string) *ClusterServer {
	for _, server := range self.servers {
		if server.ProtobufConnectionString == connectionString {
			return server
		}
	}
	return nil
}

func (self *ClusterConfiguration) UpdateServerState(serverId uint32, state ServerState) error {
	self.serversLock.Lock()
	defer self.serversLock.Unlock()
	atomic.AddUint32(&self.ClusterVersion, uint32(1))
	for _, server := range self.servers {
		if server.id == serverId {
			if state == Running {
				self.hasRunningServers = true
			}
			server.State = state
			return nil
		}
	}
	return errors.New(fmt.Sprintf("No server with id %d", serverId))
}

func (self *ClusterConfiguration) AddPotentialServer(server *ClusterServer) {
	self.serversLock.Lock()
	defer self.serversLock.Unlock()
	server.State = Potential
	self.servers = append(self.servers, server)
	server.id = uint32(len(self.servers))
	log.Info("Added server to cluster config: %d, %s, %s", server.Id, server.RaftConnectionString, server.ProtobufConnectionString)
	log.Info("Checking whether this is the local server new: %s, local: %s\n", self.config.ProtobufConnectionString(), server.ProtobufConnectionString)
	if server.ProtobufConnectionString != self.config.ProtobufConnectionString() {
		log.Info("Connecting to ProtobufServer: %s", server.ProtobufConnectionString)
		server.connection = self.connectionCreator(server.ProtobufConnectionString)
		server.Connect()
	} else if !self.addedLocalServer {
		log.Info("Added the local server")
		self.LocalServerId = server.id
		self.addedLocalServerWait <- true
		self.addedLocalServer = true
	}
}

func (self *ClusterConfiguration) GetDatabases() []*Database {
	self.createDatabaseLock.RLock()
	defer self.createDatabaseLock.RUnlock()

	dbs := make([]*Database, 0, len(self.DatabaseReplicationFactors))
	for name, rf := range self.DatabaseReplicationFactors {
		dbs = append(dbs, &Database{Name: name, ReplicationFactor: rf})
	}
	return dbs
}

func (self *ClusterConfiguration) CreateDatabase(name string, replicationFactor uint8) error {
	self.createDatabaseLock.Lock()
	defer self.createDatabaseLock.Unlock()

	if _, ok := self.DatabaseReplicationFactors[name]; ok {
		return fmt.Errorf("database %s exists", name)
	}
	self.DatabaseReplicationFactors[name] = replicationFactor
	return nil
}

func (self *ClusterConfiguration) DropDatabase(name string) error {
	self.createDatabaseLock.Lock()
	defer self.createDatabaseLock.Unlock()

	if _, ok := self.DatabaseReplicationFactors[name]; !ok {
		return fmt.Errorf("Database %s doesn't exist", name)
	}

	delete(self.DatabaseReplicationFactors, name)

	self.usersLock.Lock()
	defer self.usersLock.Unlock()

	delete(self.dbUsers, name)
	return nil
}

func (self *ClusterConfiguration) CreateContinuousQuery(db string, query string) error {
	self.continuousQueriesLock.Lock()
	defer self.continuousQueriesLock.Unlock()

	if self.continuousQueries == nil {
		self.continuousQueries = map[string][]*ContinuousQuery{}
	}

	if self.ParsedContinuousQueries == nil {
		self.ParsedContinuousQueries = map[string]map[uint32]*parser.SelectQuery{}
	}

	maxId := uint32(0)
	for _, query := range self.continuousQueries[db] {
		if query.Id > maxId {
			maxId = query.Id
		}
	}

	selectQuery, err := parser.ParseSelectQuery(query)
	if err != nil {
		return fmt.Errorf("Failed to parse continuous query: %s", query)
	}

	queryId := maxId + 1
	if self.ParsedContinuousQueries[db] == nil {
		self.ParsedContinuousQueries[db] = map[uint32]*parser.SelectQuery{queryId: selectQuery}
	} else {
		self.ParsedContinuousQueries[db][queryId] = selectQuery
	}
	self.continuousQueries[db] = append(self.continuousQueries[db], &ContinuousQuery{queryId, query})

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

func (self *ClusterConfiguration) GetDbUsers(db string) (names []string) {
	self.usersLock.RLock()
	defer self.usersLock.RUnlock()

	dbUsers := self.dbUsers[db]
	for name, _ := range dbUsers {
		names = append(names, name)
	}
	return
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
}

func (self *ClusterConfiguration) GetDatabaseReplicationFactor(name string) uint8 {
	self.createDatabaseLock.RLock()
	defer self.createDatabaseLock.RUnlock()
	return self.DatabaseReplicationFactors[name]
}

func (self *ClusterConfiguration) Save() ([]byte, error) {
	log.Debug("Dumping the cluster configuration")
	data := struct {
		Databases         map[string]uint8
		Admins            map[string]*ClusterAdmin
		DbUsers           map[string]map[string]*DbUser
		Servers           []*ClusterServer
		HasRunningServers bool
		LocalServerId     uint32
		ClusterVersion    uint32
	}{
		self.DatabaseReplicationFactors,
		self.clusterAdmins,
		self.dbUsers,
		self.servers,
		self.hasRunningServers,
		self.LocalServerId,
		self.ClusterVersion,
	}

	b := bytes.NewBuffer(nil)
	err := gob.NewEncoder(b).Encode(&data)
	if err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

func (self *ClusterConfiguration) Recovery(b []byte) error {
	log.Debug("Recovering the cluster configuration")
	data := struct {
		Databases         map[string]uint8
		Admins            map[string]*ClusterAdmin
		DbUsers           map[string]map[string]*DbUser
		Servers           []*ClusterServer
		HasRunningServers bool
		LocalServerId     uint32
		ClusterVersion    uint32
	}{}

	err := gob.NewDecoder(bytes.NewReader(b)).Decode(&data)
	if err != nil {
		return err
	}

	self.DatabaseReplicationFactors = data.Databases
	self.clusterAdmins = data.Admins
	self.dbUsers = data.DbUsers

	// copy the protobuf client from the old servers
	oldServers := map[string]ServerConnection{}
	for _, server := range self.servers {
		oldServers[server.ProtobufConnectionString] = server.connection
	}

	self.servers = data.Servers
	for _, server := range self.servers {
		server.connection = oldServers[server.ProtobufConnectionString]
		if server.connection == nil {
			server.connection = self.connectionCreator(server.ProtobufConnectionString)
			if server.ProtobufConnectionString != self.config.ProtobufConnectionString() {
				server.Connect()
			}
		}
	}

	self.hasRunningServers = data.HasRunningServers
	self.LocalServerId = data.LocalServerId
	self.ClusterVersion = data.ClusterVersion

	if self.addedLocalServer {
		return nil
	}

	for _, server := range self.servers {
		log.Info("Checking whether this is the local server new: %s, local: %s\n", self.config.ProtobufConnectionString(), server.ProtobufConnectionString)
		if server.ProtobufConnectionString != self.config.ProtobufConnectionString() {
			continue
		}
		log.Info("Added the local server")
		self.addedLocalServerWait <- true
		self.addedLocalServer = true
		break
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

func (self *ClusterConfiguration) GetShardToWriteToBySeriesAndTime(db, series string, microsecondsEpoch int64) (Shard, error) {
	shard := NewShard(self.LocalServerId, time.Now(), time.Now(), self.wal)
	shard.SetServers([]*ClusterServer{})
	shard.SetLocalStore(self.shardStore)
	return shard, nil
}
