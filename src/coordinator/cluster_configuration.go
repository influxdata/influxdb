package coordinator

import (
	"common"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

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
	databaseReplicationFactors map[string]uint8
	usersLock                  sync.RWMutex
	clusterAdmins              map[string]*clusterAdmin
	dbUsers                    map[string]map[string]*dbUser
	servers                    []*ClusterServer
	serversLock                sync.RWMutex
	hasRunningServers          bool
	currentServerId            uint32
	localServerId              uint32
	ClusterVersion             uint32
}

type Database struct {
	Name              string `json:"name"`
	ReplicationFactor uint8  `json:"replicationFactor"`
}

func NewClusterConfiguration() *ClusterConfiguration {
	return &ClusterConfiguration{
		databaseReplicationFactors: make(map[string]uint8),
		clusterAdmins:              make(map[string]*clusterAdmin),
		dbUsers:                    make(map[string]map[string]*dbUser),
		servers:                    make([]*ClusterServer, 0),
	}
}

func (self *ClusterConfiguration) IsSingleServer() bool {
	return len(self.servers) < 2
}

func (self *ClusterConfiguration) Servers() []*ClusterServer {
	return self.servers
}

func (self *ClusterConfiguration) GetReplicationFactor(database *string) uint8 {
	return self.databaseReplicationFactors[*database]
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
		if server.Id == *id {
			return server
		}
	}
	return nil
}

type serverToQuery struct {
	server               *ClusterServer
	ringLocationsToQuery uint32
}

// This function will return an array of servers to query and the number of ring locations to return per server.
// Queries are issued to every nth server in the cluster where n is the replication factor. We need the local host id
// because we want to issue the query locally and the nth servers from there.
// Optimally, the number of servers in a cluster will be evenly divisible by the replication factors used. For example,
// if you have a cluster with databases with RFs of 1, 2, and 3: optimal cluster sizes would be 6, 12, 18, 24, 30, etc.
// If that's not the case, one or more servers will have to filter out data from other servers on the fly, which could
// be a little more expensive.
func (self *ClusterConfiguration) GetServersToMakeQueryTo(localHostId uint32, database *string) (servers []*serverToQuery, replicationFactor uint32) {
	replicationFactor = uint32(self.GetReplicationFactor(database))
	replicationFactorInt := int(replicationFactor)
	index := 0
	for i, s := range self.servers {
		if s.Id == localHostId {
			index = i % replicationFactorInt
			break
		}
	}
	servers = make([]*serverToQuery, 0, len(self.servers)/replicationFactorInt)
	serverCount := len(self.servers)
	for ; index < serverCount; index += replicationFactorInt {
		server := self.servers[index]
		servers = append(servers, &serverToQuery{server, replicationFactor})
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
			servers[0].ringLocationsToQuery = uint32(replicationFactorInt - 1)
			servers = append(servers, &serverToQuery{self.servers[0], replicationFactor})
		} else {
			servers[0].ringLocationsToQuery = uint32(replicationFactorInt - 1)
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
		if s.Id == self.localServerId {
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
			if s.Id == server.Id {
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
	return &self.servers[self.GetServerIndexByLocation(location)].Id
}

func (self *ClusterConfiguration) GetServersByRingLocation(database *string, location *int) []*ClusterServer {
	index := self.GetServerIndexByLocation(location)
	_, replicas := self.GetServersByIndexAndReplicationFactor(database, &index)
	return replicas
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
		if server.Id == serverId {
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
	self.serversLock.Unlock()
	server.State = Potential
	server.Id = self.currentServerId + 1
	self.currentServerId += 1
	self.servers = append(self.servers, server)
}

func (self *ClusterConfiguration) GetDatabases() []*Database {
	self.createDatabaseLock.RLock()
	defer self.createDatabaseLock.RUnlock()

	dbs := make([]*Database, 0, len(self.databaseReplicationFactors))
	for name, rf := range self.databaseReplicationFactors {
		dbs = append(dbs, &Database{Name: name, ReplicationFactor: rf})
	}
	return dbs
}

func (self *ClusterConfiguration) CreateDatabase(name string, replicationFactor uint8) error {
	self.createDatabaseLock.Lock()
	defer self.createDatabaseLock.Unlock()

	if _, ok := self.databaseReplicationFactors[name]; ok {
		return fmt.Errorf("database %s exists", name)
	}
	self.databaseReplicationFactors[name] = replicationFactor
	return nil
}

func (self *ClusterConfiguration) DropDatabase(name string) error {
	self.createDatabaseLock.Lock()
	defer self.createDatabaseLock.Unlock()

	if _, ok := self.databaseReplicationFactors[name]; !ok {
		return fmt.Errorf("Database %s doesn't exist", name)
	}

	delete(self.databaseReplicationFactors, name)

	self.usersLock.Lock()
	defer self.usersLock.Unlock()

	delete(self.dbUsers, name)
	return nil
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

func (self *ClusterConfiguration) GetDbUser(db, username string) *dbUser {
	self.usersLock.RLock()
	defer self.usersLock.RUnlock()

	dbUsers := self.dbUsers[db]
	if dbUsers == nil {
		return nil
	}
	return dbUsers[username]
}

func (self *ClusterConfiguration) SaveDbUser(u *dbUser) {
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
		dbUsers = map[string]*dbUser{}
		self.dbUsers[db] = dbUsers
	}
	dbUsers[u.GetName()] = u
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

func (self *ClusterConfiguration) GetClusterAdmin(username string) *clusterAdmin {
	self.usersLock.RLock()
	defer self.usersLock.RUnlock()

	return self.clusterAdmins[username]
}

func (self *ClusterConfiguration) SaveClusterAdmin(u *clusterAdmin) {
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
	return self.databaseReplicationFactors[name]
}
