package coordinator

import (
	"errors"
	"fmt"
	"sync"
)

type ClusterConfiguration struct {
	createDatabaseLock sync.RWMutex
	databaseNames      map[string]bool
	usersLock          sync.RWMutex
	clusterAdmins      map[string]*clusterAdmin
	dbUsers            map[string]map[string]*dbUser
	servers            []*ClusterServer
	serversLock        sync.RWMutex
	hasRunningServers  bool
	currentServerId    uint32
}

const NUMBER_OF_RING_LOCATIONS = 10000

func NewClusterConfiguration() *ClusterConfiguration {
	return &ClusterConfiguration{
		databaseNames: make(map[string]bool),
		clusterAdmins: make(map[string]*clusterAdmin),
		dbUsers:       make(map[string]map[string]*dbUser),
		servers:       make([]*ClusterServer, 0),
	}
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

func (self *ClusterConfiguration) GetDatabases() []string {
	self.createDatabaseLock.RLock()
	defer self.createDatabaseLock.RUnlock()

	names := make([]string, 0, len(self.databaseNames))
	for name, _ := range self.databaseNames {
		names = append(names, name)
	}
	return names
}

func (self *ClusterConfiguration) CreateDatabase(name string) error {
	self.createDatabaseLock.Lock()
	defer self.createDatabaseLock.Unlock()

	if _, ok := self.databaseNames[name]; ok {
		return fmt.Errorf("database %s exists", name)
	}
	self.databaseNames[name] = true
	return nil
}

func (self *ClusterConfiguration) DropDatabase(name string) error {
	self.createDatabaseLock.Lock()
	defer self.createDatabaseLock.Unlock()

	if _, ok := self.databaseNames[name]; !ok {
		return fmt.Errorf("Database %s doesn't exist", name)
	}

	delete(self.databaseNames, name)

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
