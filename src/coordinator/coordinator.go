package coordinator

import (
	"common"
	"datastore"
	"errors"
	"fmt"
	"parser"
	"protocol"
	"sync/atomic"
)

type CoordinatorImpl struct {
	clusterConfiguration *ClusterConfiguration
	raftServer           ClusterConsensus
	datastore            datastore.Datastore
	localHostId          uint32
	requestId            uint32
}

var proxyWrite = protocol.Request_PROXY_WRITE

// this is the key used for the persistent atomic ints for sequence numbers
const POINT_SEQUENCE_NUMBER_KEY = "p"

// actual point sequence numbers will have the first part of the number
// be a host id. This ensures that sequence numbers are unique across the cluster
const HOST_ID_OFFSET = uint64(10000)

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
	if len(series.Points) == 0 {
		return fmt.Errorf("Can't write series with zero points.")
	}

	if self.clusterConfiguration.IsSingleServer() {
		_, err := self.writeSeriesToLocalStore(&db, series)
		return err
	}

	// break the series object into separate ones based on their ring location

	// if times server assigned, all the points will go to the same place
	serverAssignedTime := true
	now := common.CurrentTime()

	// assign sequence numbers
	lastNumber, err := self.datastore.AtomicIncrement(POINT_SEQUENCE_NUMBER_KEY, len(series.Points))
	if err != nil {
		return err
	}
	for _, p := range series.Points {
		if p.Timestamp == nil {
			p.Timestamp = &now
		} else {
			serverAssignedTime = false
		}
		if p.SequenceNumber == nil {
			n := self.sequenceNumberWithServerId(lastNumber)
			lastNumber--
			p.SequenceNumber = &n
		}
	}

	if serverAssignedTime {
		location := common.RingLocation(&db, series.Name, series.Points[0].Timestamp)
		i := self.clusterConfiguration.GetServerIndexByLocation(&location)
		return self.handleClusterWrite(&i, &db, series)
	}

	// TODO: make this more efficient and not suck so much
	// not all the same, so break things up

	seriesToServerIndex := make(map[int]*protocol.Series)
	for _, p := range series.Points {
		location := common.RingLocation(&db, series.Name, p.Timestamp)
		i := self.clusterConfiguration.GetServerIndexByLocation(&location)
		s := seriesToServerIndex[i]
		if s == nil {
			s = &protocol.Series{Name: series.Name, Fields: series.Fields, Points: make([]*protocol.Point, 0)}
			seriesToServerIndex[i] = s
		}
		s.Points = append(s.Points, p)
	}

	for serverIndex, s := range seriesToServerIndex {
		err := self.handleClusterWrite(&serverIndex, &db, s)
		if err != nil {
			return err
		}
	}
	return nil
}

func (self *CoordinatorImpl) writeSeriesToLocalStore(db *string, series *protocol.Series) (*protocol.Request, error) {
	request := &protocol.Request{Type: &proxyWrite, Database: db, Series: series}
	err := self.datastore.LogRequestAndAssignId(request)
	if err != nil {
		return nil, err
	}

	return request, self.datastore.WriteSeriesData(*db, series)
}

func (self *CoordinatorImpl) handleClusterWrite(serverIndex *int, db *string, series *protocol.Series) error {
	servers := self.clusterConfiguration.GetServersByIndexAndReplicationFactor(db, serverIndex)
	for _, s := range servers {
		if s.Id == self.localHostId {
			request, err := self.writeSeriesToLocalStore(db, series)
			if err != nil {
				return self.proxyUntilSuccess(servers, db, series)
			}
			self.sendRequestToReplicas(request, servers)

			return nil
		}
	}

	// it didn't live locally so proxy it
	return self.proxyUntilSuccess(servers, db, series)
}

// This method will attemp to proxy the request until the call to proxy returns nil. If no server succeeds,
// the last err value will be returned.
func (self *CoordinatorImpl) proxyUntilSuccess(servers []*ClusterServer, db *string, series *protocol.Series) (err error) {
	for _, s := range servers {
		if s.Id != self.localHostId {
			err = self.proxyWrite(s, db, series)
			if err == nil {
				return nil
			}
		}
	}
	return
}

func (self *CoordinatorImpl) proxyWrite(clusterServer *ClusterServer, db *string, series *protocol.Series) error {
	id := atomic.AddUint32(&self.requestId, uint32(1))
	request := &protocol.Request{Database: db, Type: &proxyWrite, Series: series, Id: &id}
	responseChan := make(chan *protocol.Response, 1)
	clusterServer.protobufClient.MakeRequest(request, responseChan)
	response := <-responseChan
	if *response.Type == protocol.Response_WRITE_OK {
		return nil
	} else {
		return errors.New(response.GetErrorMessage())
	}
}

func (self *CoordinatorImpl) CreateDatabase(user common.User, db string, replicationFactor uint8) error {
	if !user.IsClusterAdmin() {
		return common.NewAuthorizationError("Insufficient permission to create database")
	}

	err := self.raftServer.CreateDatabase(db, replicationFactor)
	if err != nil {
		return err
	}
	return nil
}

func (self *CoordinatorImpl) ListDatabases(user common.User) ([]*Database, error) {
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

	self.clusterConfiguration.CreateDatabase(db, uint8(1)) // ignore the error since the db may exist
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

func (self *CoordinatorImpl) ConnectToProtobufServers(localConnectionString string) error {
	// We shouldn't hit this. It's possible during initialization if Raft hasn't
	// finished spinning up then there won't be any servers in the cluster config.
	if len(self.clusterConfiguration.Servers()) == 0 {
		return errors.New("No Protobuf servers to connect to.")
	}
	for _, server := range self.clusterConfiguration.Servers() {
		if server.ProtobufConnectionString != localConnectionString {
			server.Connect()
		} else {
			fmt.Println("Coordinator: setting local id: ", server.Id)
			self.localHostId = server.Id
		}
	}
	return nil
}

func (self *CoordinatorImpl) ReplicateWrite(request *protocol.Request) error {
	location := common.RingLocation(request.Database, request.Series.Name, request.Series.Points[0].Timestamp)
	replicas := self.clusterConfiguration.GetServersByRingLocation(request.Database, &location)
	self.sendRequestToReplicas(request, replicas)
	return nil
}

func (self *CoordinatorImpl) sendRequestToReplicas(request *protocol.Request, replicas []*ClusterServer) {
	request.Type = &replicateWrite
	for _, server := range replicas {
		if server.Id != self.localHostId {
			server.protobufClient.MakeRequest(request, nil)
		}
	}
}

func (self *CoordinatorImpl) sequenceNumberWithServerId(n uint64) uint64 {
	return n*HOST_ID_OFFSET + uint64(self.localHostId)
}
