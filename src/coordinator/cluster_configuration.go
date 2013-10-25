package coordinator

import (
	"fmt"
	"sync"
)

type ClusterConfiguration struct {
	createDatabaseLock      sync.RWMutex
	databaseNames           map[string]bool
	usersLock               sync.RWMutex
	clusterAdmins           map[string]*clusterAdmin
	dbUsers                 map[string]map[string]*dbUser
	activeServerConfig      []*ClusterServer
	potentialServerConfig   []*ClusterServer
	rebalancingServerConfig []*ClusterServer
}

const NUMBER_OF_RING_LOCATIONS = 10000

func NewClusterConfiguration() *ClusterConfiguration {
	return &ClusterConfiguration{
		databaseNames:           make(map[string]bool),
		clusterAdmins:           make(map[string]*clusterAdmin),
		dbUsers:                 make(map[string]map[string]*dbUser),
		activeServerConfig:      make([]*ClusterServer, 0),
		potentialServerConfig:   make([]*ClusterServer, 0),
		rebalancingServerConfig: make([]*ClusterServer, 0),
	}
}

func (self *ClusterConfiguration) AddPotentialServer(server *ClusterServer) {
	self.potentialServerConfig = append(self.potentialServerConfig, server)
}

func (self *ClusterConfiguration) RebalanceBasedOnPotentialConfig() {
}

func (self *ClusterConfiguration) UpdatePotentialServerOrder(serverIds []uint32) {
}

func (self *ClusterConfiguration) MoveRebalancingToActive() {
	self.activeServerConfig = self.rebalancingServerConfig
	self.rebalancingServerConfig = make([]*ClusterServer, 0)
	self.potentialServerConfig = self.activeServerConfig
}

func (self *ClusterConfiguration) GetDatabases() map[string]bool {
	self.createDatabaseLock.RLock()
	defer self.createDatabaseLock.RUnlock()

	names := make(map[string]bool)
	for name, _ := range self.databaseNames {
		names[name] = true
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
	return nil
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

func (self *ClusterConfiguration) SaveClusterAdmin(u *clusterAdmin) {
	self.usersLock.Lock()
	defer self.usersLock.Unlock()
	if u.IsDeleted() {
		delete(self.clusterAdmins, u.GetName())
		return
	}
	self.clusterAdmins[u.GetName()] = u
}
