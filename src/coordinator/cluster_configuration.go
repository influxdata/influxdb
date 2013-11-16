package coordinator

import (
	"errors"
	"fmt"
	"sync"
)

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

func (self *ClusterConfiguration) GetServerByRaftName(name string) *ClusterServer {
	for _, server := range self.servers {
		if server.RaftName == name {
			return server
		}
	}
	return nil
}

func (self *ClusterConfiguration) GetServerIndexByLocation(location *int) int {
	return *location % len(self.servers)
}

func (self *ClusterConfiguration) GetServersByRingLocation(database *string, location *int) []*ClusterServer {
	index := *location % len(self.servers)
	return self.GetServersByIndexAndReplicationFactor(database, &index)
}

func (self *ClusterConfiguration) GetServersByIndexAndReplicationFactor(database *string, index *int) []*ClusterServer {
	replicationFactor := self.GetReplicationFactor(database)
	serverCount := len(self.servers)
	if int(replicationFactor) >= serverCount {
		return self.servers
	}
	owners := make([]*ClusterServer, 0, replicationFactor)
	for i := *index; i < serverCount; i++ {
		owners = append(owners, self.servers[i])
	}
	ownerCount := len(owners)
	for i := 0; ownerCount < int(replicationFactor); i++ {
		owners = append(owners, self.servers[i])
		ownerCount++
	}
	return owners
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
