package coordinator

import (
	log "code.google.com/p/log4go"
	"common"
	"datastore"
	"errors"
	"fmt"
	"math"
	"parser"
	"protocol"
	"sync"
	"sync/atomic"
)

type CoordinatorImpl struct {
	clusterConfiguration *ClusterConfiguration
	raftServer           ClusterConsensus
	datastore            datastore.Datastore
	localHostId          uint32
	requestId            uint32
	runningReplays       map[string][]*protocol.Request
	runningReplaysLock   sync.Mutex
}

// this is the key used for the persistent atomic ints for sequence numbers
const POINT_SEQUENCE_NUMBER_KEY = "p"

// actual point sequence numbers will have the first part of the number
// be a host id. This ensures that sequence numbers are unique across the cluster
const HOST_ID_OFFSET = uint64(10000)

var (
	BARRIER_TIME_MIN int64 = math.MinInt64
	BARRIER_TIME_MAX int64 = math.MaxInt64
)

// shorter constants for readability
var (
	proxyWrite        = protocol.Request_PROXY_WRITE
	queryRequest      = protocol.Request_QUERY
	endStreamResponse = protocol.Response_END_STREAM
	queryResponse     = protocol.Response_QUERY
	replayReplication = protocol.Request_REPLICATION_REPLAY
)

func NewCoordinatorImpl(datastore datastore.Datastore, raftServer ClusterConsensus, clusterConfiguration *ClusterConfiguration) *CoordinatorImpl {
	return &CoordinatorImpl{
		clusterConfiguration: clusterConfiguration,
		raftServer:           raftServer,
		datastore:            datastore,
		runningReplays:       make(map[string][]*protocol.Request),
	}
}

// Distributes the query across the cluster and combines the results. Yields as they come in ensuring proper order.
// TODO: make this work even if there is a downed server in the cluster
func (self *CoordinatorImpl) DistributeQuery(user common.User, db string, query *parser.Query, yield func(*protocol.Series) error) error {
	if self.clusterConfiguration.IsSingleServer() {
		return self.datastore.ExecuteQuery(user, db, query, yield, nil)
	}
	servers, replicationFactor := self.clusterConfiguration.GetServersToMakeQueryTo(self.localHostId, &db)
	queryString := query.GetQueryString()
	id := atomic.AddUint32(&self.requestId, uint32(1))
	userName := user.GetName()
	responseChannels := make([]chan *protocol.Response, 0, len(servers)+1)
	var localServerToQuery *serverToQuery
	for _, server := range servers {
		if server.server.Id == self.localHostId {
			localServerToQuery = server
		} else {
			request := &protocol.Request{Type: &queryRequest, Query: &queryString, Id: &id, Database: &db, UserName: &userName}
			if server.ringLocationsToQuery != replicationFactor {
				r := server.ringLocationsToQuery
				request.RingLocationsToQuery = &r
			}
			responseChan := make(chan *protocol.Response, 3)
			server.server.protobufClient.MakeRequest(request, responseChan)
			responseChannels = append(responseChannels, responseChan)
		}
	}

	local := make(chan *protocol.Response)
	var nextPoint *protocol.Point
	chanClosed := false
	sendFromLocal := func(series *protocol.Series) error {
		pointCount := len(series.Points)
		if pointCount == 0 {
			if nextPoint != nil {
				series.Points = append(series.Points, nextPoint)
			}

			if !chanClosed {
				local <- &protocol.Response{Type: &endStreamResponse, Series: series}
				chanClosed = true
				close(local)
			}
			return nil
		}
		oldNextPoint := nextPoint
		nextPoint = series.Points[pointCount-1]
		series.Points[pointCount-1] = nil
		if oldNextPoint != nil {
			copy(series.Points[1:], series.Points[0:])
			series.Points[0] = oldNextPoint
		} else {
			series.Points = series.Points[:len(series.Points)-1]
		}

		response := &protocol.Response{Series: series, Type: &queryResponse}
		if nextPoint != nil {
			response.NextPointTime = nextPoint.Timestamp
		}
		local <- response
		return nil
	}
	responseChannels = append(responseChannels, local)
	// TODO: wire up the willreturnsingleseries method and uncomment this line and delete the next one.
	//	isSingleSeriesQuery := query.WillReturnSingleSeries()
	isSingleSeriesQuery := false

	go func() {
		var ringFilter func(database, series *string, time *int64) bool
		if replicationFactor != localServerToQuery.ringLocationsToQuery {
			ringFilter = self.clusterConfiguration.GetRingFilterFunction(db, localServerToQuery.ringLocationsToQuery)
		}
		self.datastore.ExecuteQuery(user, db, query, sendFromLocal, ringFilter)
	}()
	self.streamResultsFromChannels(isSingleSeriesQuery, query.Ascending, responseChannels, yield)
	return nil
}

// This function streams results from servers and ensures that series are yielded in the proper order (which is expected by the engine)
func (self *CoordinatorImpl) streamResultsFromChannels(isSingleSeriesQuery, isAscending bool, channels []chan *protocol.Response, yield func(*protocol.Series) error) {
	channelCount := len(channels)
	closedChannels := 0
	responses := make([]*protocol.Response, 0)
	var leftovers []*protocol.Series

	seriesNames := make(map[string]bool)
	for closedChannels < channelCount {
		for _, ch := range channels {
			response := <-ch
			if response != nil {
				if *response.Type == protocol.Response_END_STREAM {
					closedChannels++
				}
				seriesNames[*response.Series.Name] = true
				if response.Series.Points != nil {
					responses = append(responses, response)
				}
			}
		}
		leftovers = self.yieldResults(isSingleSeriesQuery, isAscending, leftovers, responses, yield)
		responses = make([]*protocol.Response, 0)
	}
	for _, leftover := range leftovers {
		if len(leftover.Points) > 0 {
			yield(leftover)
		}
	}
	for n, _ := range seriesNames {
		name := n
		yield(&protocol.Series{Name: &name, Points: []*protocol.Point{}})
	}
}

// Response objects have a nextPointTime that tells us what the time of the next series from a given server will be.
// Using that we can make sure to yield results in the correct order. So we can safely yield all results that fall before
// (or after if descending) the lowest (or highest if descending) nextPointTime. If they're all nil, then we're safe to
// yield everything
func (self *CoordinatorImpl) yieldResults(isSingleSeriesQuery, isAscending bool, leftovers []*protocol.Series,
	responses []*protocol.Response, yield func(*protocol.Series) error) []*protocol.Series {

	if isSingleSeriesQuery {
		var oldLeftOver *protocol.Series
		if len(leftovers) > 0 {
			oldLeftOver = leftovers[0]
		}
		leftover := self.yieldResultsForSeries(isAscending, oldLeftOver, responses, yield)
		if leftover != nil {
			return []*protocol.Series{leftover}
		}
		return nil
	}

	// a query could yield results from multiple series, handle the cases individually
	nameToSeriesResponses := make(map[string][]*protocol.Response)
	for _, response := range responses {
		seriesResponses := nameToSeriesResponses[*response.Series.Name]
		if seriesResponses == nil {
			seriesResponses = make([]*protocol.Response, 0)
		}
		nameToSeriesResponses[*response.Series.Name] = append(seriesResponses, response)
	}
	leftoverResults := make([]*protocol.Series, 0)
	for _, responses := range nameToSeriesResponses {
		response := responses[0]
		var seriesLeftover *protocol.Series
		for _, series := range leftovers {
			if *series.Name == *response.Series.Name {
				seriesLeftover = series
				break
			}
		}
		leftover := self.yieldResultsForSeries(isAscending, seriesLeftover, responses, yield)
		if leftover != nil {
			leftoverResults = append(leftoverResults, leftover)
		}
	}
	return leftoverResults
}

// Function yields all results that are safe to do so ensuring order. Returns all results that must wait for more from the servers.
func (self *CoordinatorImpl) yieldResultsForSeries(isAscending bool, leftover *protocol.Series, responses []*protocol.Response, yield func(*protocol.Series) error) *protocol.Series {
	result := &protocol.Series{Name: responses[0].Series.Name, Points: make([]*protocol.Point, 0)}
	if leftover == nil {
		leftover = &protocol.Series{Name: responses[0].Series.Name, Points: make([]*protocol.Point, 0)}
	}

	barrierTime := BARRIER_TIME_MIN
	if isAscending {
		barrierTime = BARRIER_TIME_MAX
	}
	var shouldYieldComparator func(rawTime *int64) bool
	if isAscending {
		shouldYieldComparator = func(rawTime *int64) bool {
			if rawTime != nil && *rawTime < barrierTime {
				return true
			} else {
				return false
			}
		}
	} else {
		shouldYieldComparator = func(rawTime *int64) bool {
			if rawTime != nil && *rawTime > barrierTime {
				return true
			} else {
				return false
			}
		}
	}
	// find the barrier time
	for _, response := range responses {
		if shouldYieldComparator(response.NextPointTime) {
			barrierTime = *response.NextPointTime
		}
	}
	// yield the points from leftover that are safe
	for _, point := range leftover.Points {
		if shouldYieldComparator(point.Timestamp) {
			result.Points = append(result.Points, point)
		} else {
			break
		}
	}
	// if they all got added, clear out the leftover
	if len(leftover.Points) == len(result.Points) {
		leftover.Points = make([]*protocol.Point, 0)
	}

	if barrierTime == BARRIER_TIME_MIN || barrierTime == BARRIER_TIME_MAX {
		// all the nextPointTimes were nil so we're safe to send everything
		for _, response := range responses {
			result.Points = append(result.Points, response.Series.Points...)
			result.Points = append(result.Points, leftover.Points...)
			leftover.Points = []*protocol.Point{}
		}
	} else {
		for _, response := range responses {
			if shouldYieldComparator(response.NextPointTime) {
				// all points safe to yield
				result.Points = append(result.Points, response.Series.Points...)
				continue
			}

			for i, point := range response.Series.Points {
				if shouldYieldComparator(point.Timestamp) {
					result.Points = append(result.Points, point)
				} else {
					// since they're returned in order, we can just append these to
					// the leftover and break out.
					leftover.Points = append(leftover.Points, response.Series.Points[i:]...)
					break
				}
			}
		}
	}

	if isAscending {
		result.SortPointsTimeAscending()
		leftover.SortPointsTimeAscending()
	} else {
		result.SortPointsTimeDescending()
		leftover.SortPointsTimeDescending()
	}

	// Don't yield an empty points array, the engine will think it's the end of the stream.
	// streamResultsFromChannels will send the empty ones after all channels have returned.
	if len(result.Points) > 0 {
		yield(result)
	}
	if len(leftover.Points) > 0 {
		return leftover
	}
	return nil
}

func (self *CoordinatorImpl) ReplayReplication(request *protocol.Request, replicationFactor *uint8, owningServerId *uint32, lastSeenSequenceNumber *uint64) {
	key := fmt.Sprintf("%d_%d_%d_%d", *replicationFactor, *request.ClusterVersion, *request.OriginatingServerId, *owningServerId)
	self.runningReplaysLock.Lock()
	requestsWaitingToWrite := self.runningReplays[key]
	if requestsWaitingToWrite != nil {
		self.runningReplays[key] = append(requestsWaitingToWrite, request)
		self.runningReplaysLock.Unlock()
		return
	}
	self.runningReplays[key] = []*protocol.Request{request}
	self.runningReplaysLock.Unlock()

	id := atomic.AddUint32(&self.requestId, uint32(1))
	replicationFactor32 := uint32(*replicationFactor)
	database := ""
	replayRequest := &protocol.Request{
		Id:                      &id,
		Type:                    &replayReplication,
		Database:                &database,
		ReplicationFactor:       &replicationFactor32,
		OriginatingServerId:     request.OriginatingServerId,
		OwnerServerId:           owningServerId,
		ClusterVersion:          request.ClusterVersion,
		LastKnownSequenceNumber: lastSeenSequenceNumber}
	replayedRequests := make(chan *protocol.Response, 100)
	server := self.clusterConfiguration.GetServerById(request.OriginatingServerId)
	log.Error("COORD REPLAY: ", request, server, self.localHostId)
	err := server.protobufClient.MakeRequest(replayRequest, replayedRequests)
	if err != nil {
		log.Error("COORD REPLAY ERROR: ", err)
		return
	}
	for {
		response := <-replayedRequests
		if response == nil || *response.Type == protocol.Response_REPLICATION_REPLAY_END {
			self.runningReplaysLock.Lock()
			defer self.runningReplaysLock.Unlock()
			for _, r := range self.runningReplays[key] {
				err := self.datastore.LogRequestAndAssignSequenceNumber(r, replicationFactor, owningServerId)
				if err != nil {
					log.Error("Error writing waiting requests after replay: ", err)
				}
				self.datastore.WriteSeriesData(*r.Database, r.Series)
			}
			delete(self.runningReplays, key)
			log.Info("Replay done for originating server %d and owner server %d", *request.OriginatingServerId, *owningServerId)
			return
		}
		request := response.Request
		// TODO: make request logging and datastore write atomic
		err := self.datastore.LogRequestAndAssignSequenceNumber(request, replicationFactor, owningServerId)
		if err != nil {
			log.Error("ERROR writing replay: ", err)
		} else {
			self.datastore.WriteSeriesData(*request.Database, request.Series)
		}
	}
}

func (self *CoordinatorImpl) WriteSeriesData(user common.User, db string, series *protocol.Series) error {
	if !user.HasWriteAccess(db) {
		return common.NewAuthorizationError("Insufficient permission to write to %s", db)
	}
	if len(series.Points) == 0 {
		return fmt.Errorf("Can't write series with zero points.")
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
	lastNumber = lastNumber - uint64(len(series.Points)-1)
	for _, p := range series.Points {
		if p.Timestamp == nil {
			p.Timestamp = &now
		} else {
			serverAssignedTime = false
		}
		if p.SequenceNumber == nil {
			n := self.sequenceNumberWithServerId(lastNumber)
			lastNumber++
			p.SequenceNumber = &n
		}
	}

	// if it's a single server setup, we don't need to bother with getting ring
	// locations or logging requests or any of that, so just write to the local db and be done.
	if self.clusterConfiguration.IsSingleServer() {
		err := self.writeSeriesToLocalStore(&db, series)
		return err
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

func (self *CoordinatorImpl) writeSeriesToLocalStore(db *string, series *protocol.Series) error {
	return self.datastore.WriteSeriesData(*db, series)
}

func (self *CoordinatorImpl) handleClusterWrite(serverIndex *int, db *string, series *protocol.Series) error {
	owner, servers := self.clusterConfiguration.GetServersByIndexAndReplicationFactor(db, serverIndex)
	for _, s := range servers {
		if s.Id == self.localHostId {
			// TODO: make storing of the data and logging of the request atomic
			id := atomic.AddUint32(&self.requestId, uint32(1))
			request := &protocol.Request{Type: &proxyWrite, Database: db, Series: series, Id: &id}
			request.OriginatingServerId = &self.localHostId
			request.ClusterVersion = &self.clusterConfiguration.ClusterVersion
			replicationFactor := self.clusterConfiguration.GetReplicationFactor(db)
			err := self.datastore.LogRequestAndAssignSequenceNumber(request, &replicationFactor, &owner.Id)
			if err != nil {
				return self.proxyUntilSuccess(servers, db, series)
			}

			// ignoring the error for writing to the local store because we still want to send to replicas
			err = self.writeSeriesToLocalStore(db, series)
			if err != nil {
				log.Error("Couldn't write data to local store: ", err, request)
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
	request.ClusterVersion = &self.clusterConfiguration.ClusterVersion
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
			self.localHostId = server.Id
			self.clusterConfiguration.localServerId = server.Id
		}
	}
	return nil
}

func (self *CoordinatorImpl) ReplicateWrite(request *protocol.Request) error {
	id := atomic.AddUint32(&self.requestId, uint32(1))
	request.Id = &id
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
