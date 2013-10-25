package coordinator

import (
	"common"
	"datastore"
	"fmt"
	"parser"
	"protocol"
	"sync"
)

type CoordinatorImpl struct {
	clusterConfiguration  *ClusterConfiguration
	raftServer            *RaftServer
	datastore             datastore.Datastore
	currentSequenceNumber uint32
	sequenceNumberLock    sync.Mutex
}

func NewCoordinatorImpl(datastore datastore.Datastore, raftServer *RaftServer, clusterConfiguration *ClusterConfiguration) *CoordinatorImpl {
	return &CoordinatorImpl{
		clusterConfiguration: clusterConfiguration,
		raftServer:           raftServer,
		datastore:            datastore,
	}
}

func (self *CoordinatorImpl) DistributeQuery(db string, query *parser.Query, yield func(*protocol.Series) error) error {
	return self.datastore.ExecuteQuery(db, query, yield)
}

func (self *CoordinatorImpl) WriteSeriesData(db string, series *protocol.Series) error {
	now := common.CurrentTime()
	for _, p := range series.Points {
		if p.Timestamp == nil {
			p.Timestamp = &now
			self.sequenceNumberLock.Lock()
			self.currentSequenceNumber += 1
			n := self.currentSequenceNumber
			self.sequenceNumberLock.Unlock()
			p.SequenceNumber = &n
		} else if p.SequenceNumber == nil {
			self.sequenceNumberLock.Lock()
			self.currentSequenceNumber += 1
			n := self.currentSequenceNumber
			self.sequenceNumberLock.Unlock()
			p.SequenceNumber = &n
		}
	}
	return self.datastore.WriteSeriesData(db, series)
}

func (self *CoordinatorImpl) CreateDatabase(db string) error {
	err := self.raftServer.CreateDatabase(db)
	if err != nil {
		return err
	}
	return nil
}

func (self *CoordinatorImpl) DropDatabase(db string) error {
	return self.raftServer.DropDatabase(db)
}

func (self *CoordinatorImpl) AuthenticateDbUser(db, username, password string) (User, error) {
	dbUsers := self.clusterConfiguration.dbUsers[db]
	if dbUsers == nil || dbUsers[username] == nil {
		return self.AuthenticateClusterAdmin(username, password)
	}
	user := dbUsers[username]
	if user.isValidPwd(password) {
		return user, nil
	}
	return nil, fmt.Errorf("Invalid username/password")
}

func (self *CoordinatorImpl) AuthenticateClusterAdmin(username, password string) (User, error) {
	user := self.clusterConfiguration.clusterAdmins[username]
	if user == nil {
		return nil, fmt.Errorf("Invalid username/password")
	}
	if user.isValidPwd(password) {
		return user, nil
	}
	return nil, fmt.Errorf("Invalid username/password")
}

func (self *CoordinatorImpl) CreateClusterAdminUser(requester User, username string) error {
	if !requester.IsClusterAdmin() {
		return fmt.Errorf("Insufficient permissions")
	}

	if self.clusterConfiguration.clusterAdmins[username] != nil {
		return fmt.Errorf("User %s already exists", username)
	}

	return self.raftServer.SaveClusterAdminUser(&clusterAdmin{CommonUser{Name: username}})
}

func (self *CoordinatorImpl) DeleteClusterAdminUser(requester User, username string) error {
	if !requester.IsClusterAdmin() {
		return fmt.Errorf("Insufficient permissions")
	}

	user := self.clusterConfiguration.clusterAdmins[username]
	if user == nil {
		return fmt.Errorf("User %s doesn't exists", username)
	}

	user.CommonUser.IsUserDeleted = true
	return self.raftServer.SaveClusterAdminUser(user)
}

func (self *CoordinatorImpl) ChangeClusterAdminPassword(requester User, username, password string) error {
	if !requester.IsClusterAdmin() {
		return fmt.Errorf("Insufficient permissions")
	}

	user := self.clusterConfiguration.clusterAdmins[username]
	if user == nil {
		return fmt.Errorf("Invalid user name %s", username)
	}

	user.changePassword(password)
	return self.raftServer.SaveClusterAdminUser(user)
}

func (self *CoordinatorImpl) CreateDbUser(requester User, db, username string) error {
	if !requester.IsClusterAdmin() && !requester.IsDbAdmin(db) {
		return fmt.Errorf("Insufficient permissions")
	}

	dbUsers := self.clusterConfiguration.dbUsers[db]
	if dbUsers != nil && dbUsers[username] != nil {
		return fmt.Errorf("User %s already exists", username)
	}

	if dbUsers == nil {
		dbUsers = map[string]*dbUser{}
		self.clusterConfiguration.dbUsers[db] = dbUsers
	}

	return self.raftServer.SaveDbUser(&dbUser{CommonUser{Name: username}, db, nil, nil, false})
}

func (self *CoordinatorImpl) DeleteDbUser(requester User, db, username string) error {
	if !requester.IsClusterAdmin() && !requester.IsDbAdmin(db) {
		return fmt.Errorf("Insufficient permissions")
	}

	dbUsers := self.clusterConfiguration.dbUsers[db]
	if dbUsers == nil || dbUsers[username] == nil {
		return fmt.Errorf("User %s doesn't exists", username)
	}

	user := dbUsers[username]
	user.CommonUser.IsUserDeleted = true
	return self.raftServer.SaveDbUser(user)
}

func (self *CoordinatorImpl) ChangeDbUserPassword(requester User, db, username, password string) error {
	if !requester.IsClusterAdmin() && !requester.IsDbAdmin(db) && !(requester.GetDb() == db && requester.GetName() == username) {
		return fmt.Errorf("Insufficient permissions")
	}

	dbUsers := self.clusterConfiguration.dbUsers[db]
	if dbUsers == nil || dbUsers[username] == nil {
		return fmt.Errorf("Invalid username %s", username)
	}

	dbUsers[username].changePassword(password)
	return self.raftServer.SaveDbUser(dbUsers[username])
}

func (self *CoordinatorImpl) SetDbAdmin(requester User, db, username string, isAdmin bool) error {
	if !requester.IsClusterAdmin() && !requester.IsDbAdmin(db) {
		return fmt.Errorf("Insufficient permissions")
	}

	dbUsers := self.clusterConfiguration.dbUsers[db]
	if dbUsers == nil || dbUsers[username] == nil {
		return fmt.Errorf("Invalid username %s", username)
	}

	user := dbUsers[username]
	user.IsAdmin = isAdmin
	self.raftServer.SaveDbUser(user)
	return nil
}
