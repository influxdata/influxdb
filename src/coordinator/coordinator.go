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
	raftServer            ClusterConsensus
	datastore             datastore.Datastore
	currentSequenceNumber uint32
	sequenceNumberLock    sync.Mutex
}

func NewCoordinatorImpl(datastore datastore.Datastore, raftServer ClusterConsensus, clusterConfiguration *ClusterConfiguration) *CoordinatorImpl {
	return &CoordinatorImpl{
		clusterConfiguration: clusterConfiguration,
		raftServer:           raftServer,
		datastore:            datastore,
	}
}

func (self *CoordinatorImpl) DistributeQuery(user common.User, db string, query *parser.Query, yield func(*protocol.Series) error) error {
	return self.datastore.ExecuteQuery(user, db, query, yield)
}

func (self *CoordinatorImpl) WriteSeriesData(user common.User, db string, series *protocol.Series) error {
	if !user.HasWriteAccess(db) {
		return common.NewAuthorizationError("Insufficient permission to write to %s", db)
	}

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

func (self *CoordinatorImpl) CreateDatabase(user common.User, db string) error {
	if !user.IsClusterAdmin() {
		return common.NewAuthorizationError("Insufficient permission to create database")
	}

	err := self.raftServer.CreateDatabase(db)
	if err != nil {
		return err
	}
	return nil
}

func (self *CoordinatorImpl) ListDatabases(user common.User) ([]string, error) {
	if !user.IsClusterAdmin() {
		return nil, common.NewAuthorizationError("Insufficient permission to list databases")
	}

	dbs := self.clusterConfiguration.GetDatabases()
	return dbs, nil
}

func (self *CoordinatorImpl) DropDatabase(user common.User, db string) error {
	if !user.IsClusterAdmin() {
		return common.NewAuthorizationError("Insufficient permission to drop database")
	}

	if err := self.raftServer.DropDatabase(db); err != nil {
		return err
	}

	return self.datastore.DropDatabase(db)
}

func (self *CoordinatorImpl) AuthenticateDbUser(db, username, password string) (common.User, error) {
	dbUsers := self.clusterConfiguration.dbUsers[db]
	if dbUsers == nil || dbUsers[username] == nil {
		return self.AuthenticateClusterAdmin(username, password)
	}
	user := dbUsers[username]
	if user.isValidPwd(password) {
		return user, nil
	}
	return nil, common.NewAuthorizationError("Invalid username/password")
}

func (self *CoordinatorImpl) AuthenticateClusterAdmin(username, password string) (common.User, error) {
	user := self.clusterConfiguration.clusterAdmins[username]
	if user == nil {
		return nil, common.NewAuthorizationError("Invalid username/password")
	}
	if user.isValidPwd(password) {
		return user, nil
	}
	return nil, common.NewAuthorizationError("Invalid username/password")
}

func (self *CoordinatorImpl) ListClusterAdmins(requester common.User) ([]string, error) {
	if !requester.IsClusterAdmin() {
		return nil, common.NewAuthorizationError("Insufficient permissions")
	}

	return self.clusterConfiguration.GetClusterAdmins(), nil
}

func (self *CoordinatorImpl) CreateClusterAdminUser(requester common.User, username string) error {
	if !requester.IsClusterAdmin() {
		return common.NewAuthorizationError("Insufficient permissions")
	}

	if username == "" {
		return fmt.Errorf("Username cannot be empty")
	}

	if self.clusterConfiguration.clusterAdmins[username] != nil {
		return fmt.Errorf("User %s already exists", username)
	}

	return self.raftServer.SaveClusterAdminUser(&clusterAdmin{CommonUser{Name: username}})
}

func (self *CoordinatorImpl) DeleteClusterAdminUser(requester common.User, username string) error {
	if !requester.IsClusterAdmin() {
		return common.NewAuthorizationError("Insufficient permissions")
	}

	user := self.clusterConfiguration.clusterAdmins[username]
	if user == nil {
		return fmt.Errorf("User %s doesn't exists", username)
	}

	user.CommonUser.IsUserDeleted = true
	return self.raftServer.SaveClusterAdminUser(user)
}

func (self *CoordinatorImpl) ChangeClusterAdminPassword(requester common.User, username, password string) error {
	if !requester.IsClusterAdmin() {
		return common.NewAuthorizationError("Insufficient permissions")
	}

	user := self.clusterConfiguration.clusterAdmins[username]
	if user == nil {
		return fmt.Errorf("Invalid user name %s", username)
	}

	user.changePassword(password)
	return self.raftServer.SaveClusterAdminUser(user)
}

func (self *CoordinatorImpl) CreateDbUser(requester common.User, db, username string) error {
	if !requester.IsClusterAdmin() && !requester.IsDbAdmin(db) {
		return common.NewAuthorizationError("Insufficient permissions")
	}

	if username == "" {
		return fmt.Errorf("Username cannot be empty")
	}

	self.clusterConfiguration.CreateDatabase(db) // ignore the error since the db may exist
	dbUsers := self.clusterConfiguration.dbUsers[db]
	if dbUsers != nil && dbUsers[username] != nil {
		return fmt.Errorf("User %s already exists", username)
	}

	if dbUsers == nil {
		dbUsers = map[string]*dbUser{}
		self.clusterConfiguration.dbUsers[db] = dbUsers
	}

	matchers := []*Matcher{&Matcher{true, ".*"}}
	return self.raftServer.SaveDbUser(&dbUser{CommonUser{Name: username}, db, matchers, matchers, false})
}

func (self *CoordinatorImpl) DeleteDbUser(requester common.User, db, username string) error {
	if !requester.IsClusterAdmin() && !requester.IsDbAdmin(db) {
		return common.NewAuthorizationError("Insufficient permissions")
	}

	dbUsers := self.clusterConfiguration.dbUsers[db]
	if dbUsers == nil || dbUsers[username] == nil {
		return fmt.Errorf("User %s doesn't exists", username)
	}

	user := dbUsers[username]
	user.CommonUser.IsUserDeleted = true
	return self.raftServer.SaveDbUser(user)
}

func (self *CoordinatorImpl) ListDbUsers(requester common.User, db string) ([]string, error) {
	if !requester.IsClusterAdmin() && !requester.IsDbAdmin(db) {
		return nil, common.NewAuthorizationError("Insufficient permissions")
	}

	return self.clusterConfiguration.GetDbUsers(db), nil
}

func (self *CoordinatorImpl) ChangeDbUserPassword(requester common.User, db, username, password string) error {
	if !requester.IsClusterAdmin() && !requester.IsDbAdmin(db) && !(requester.GetDb() == db && requester.GetName() == username) {
		return common.NewAuthorizationError("Insufficient permissions")
	}

	dbUsers := self.clusterConfiguration.dbUsers[db]
	if dbUsers == nil || dbUsers[username] == nil {
		return fmt.Errorf("Invalid username %s", username)
	}

	dbUsers[username].changePassword(password)
	return self.raftServer.SaveDbUser(dbUsers[username])
}

func (self *CoordinatorImpl) SetDbAdmin(requester common.User, db, username string, isAdmin bool) error {
	if !requester.IsClusterAdmin() && !requester.IsDbAdmin(db) {
		return common.NewAuthorizationError("Insufficient permissions")
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
