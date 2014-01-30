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
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type CoordinatorImpl struct {
	clusterConfiguration *ClusterConfiguration
	raftServer           ClusterConsensus
	datastore            datastore.Datastore
	requestId            uint32
	runningReplays       map[string][]*protocol.Request
	runningReplaysLock   sync.Mutex
	writeLock            sync.Mutex
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
	proxyWrite            = protocol.Request_PROXY_WRITE
	proxyDelete           = protocol.Request_PROXY_DELETE
	proxyDropDatabase     = protocol.Request_PROXY_DROP_DATABASE
	replicateDropDatabase = protocol.Request_REPLICATION_DROP_DATABASE
	proxyDropSeries       = protocol.Request_PROXY_DROP_SERIES
	replicateDropSeries   = protocol.Request_REPLICATION_DROP_SERIES
	queryRequest          = protocol.Request_QUERY
	listSeriesRequest     = protocol.Request_LIST_SERIES
	listSeriesResponse    = protocol.Response_LIST_SERIES
	endStreamResponse     = protocol.Response_END_STREAM
	queryResponse         = protocol.Response_QUERY
	replayReplication     = protocol.Request_REPLICATION_REPLAY
	sequenceNumber        = protocol.Request_SEQUENCE_NUMBER
)

// usernames and db names should match this regex
var VALID_NAMES *regexp.Regexp

func init() {
	var err error
	VALID_NAMES, err = regexp.Compile("^[a-zA-Z0-9_][a-zA-Z0-9\\._-]*$")
	if err != nil {
		panic(err)
	}
}

func NewCoordinatorImpl(datastore datastore.Datastore, raftServer ClusterConsensus, clusterConfiguration *ClusterConfiguration) *CoordinatorImpl {
	coordinator := &CoordinatorImpl{
		clusterConfiguration: clusterConfiguration,
		raftServer:           raftServer,
		datastore:            datastore,
		runningReplays:       make(map[string][]*protocol.Request),
	}

	return coordinator
}

func (self *CoordinatorImpl) ForceCompaction(user common.User) error {
	if !user.IsClusterAdmin() {
		return fmt.Errorf("Insufficient permission to force a log compaction")
	}

	return self.raftServer.ForceLogCompaction()
}

// Distributes the query across the cluster and combines the results. Yields as they come in ensuring proper order.
// TODO: make this work even if there is a downed server in the cluster
func (self *CoordinatorImpl) DistributeQuery(user common.User, db string, query *parser.SelectQuery, localOnly bool, yield func(*protocol.Series) error) error {
	if self.clusterConfiguration.IsSingleServer() || localOnly {
		return self.datastore.ExecuteQuery(user, db, query, yield, nil)
	}
	servers, replicationFactor := self.clusterConfiguration.GetServersToMakeQueryTo(&db)
	id := atomic.AddUint32(&self.requestId, uint32(1))
	userName := user.GetName()
	isDbUser := !user.IsClusterAdmin()
	responseChannels := make([]chan *protocol.Response, 0, len(servers)+1)
	queryString := query.GetQueryString()
	var localServerToQuery *serverToQuery
	for _, server := range servers {
		if server.server.Id == self.clusterConfiguration.localServerId {
			localServerToQuery = server
		} else {
			request := &protocol.Request{Type: &queryRequest, Query: &queryString, Id: &id, Database: &db, UserName: &userName, IsDbUser: &isDbUser}
			if server.ringLocationsToQuery != replicationFactor {
				r := server.ringLocationsToQuery
				request.RingLocationsToQuery = &r
			}
			responseChan := make(chan *protocol.Response, 3)
			server.server.MakeRequest(request, responseChan)
			responseChannels = append(responseChannels, responseChan)
		}
	}

	local := make(chan *protocol.Response)
	nextPointMap := make(map[string]*NextPoint)

	// TODO: this style of wrapping the series in response objects with the
	//       last point time is duplicated in the request handler. Refactor...
	sendFromLocal := func(series *protocol.Series) error {
		response := createResponse(nextPointMap, series, nil)
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
		local <- &protocol.Response{Type: &endStreamResponse}
		close(local)
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
			if response == nil {
				continue
			}

			if *response.Type == protocol.Response_END_STREAM {
				closedChannels++
				continue
			}

			seriesNames[*response.Series.Name] = true
			if response.Series.Points != nil {
				responses = append(responses, response)
			}
		}

		if len(responses) == 0 {
			continue
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

// TODO: refactor this for clarity. This got super ugly...
// Function yields all results that are safe to do so ensuring order. Returns all results that must wait for more from the servers.
func (self *CoordinatorImpl) yieldResultsForSeries(isAscending bool, leftover *protocol.Series, responses []*protocol.Response, yield func(*protocol.Series) error) *protocol.Series {
	// results can come from different servers. Some of which won't know about fields that other servers may know about.
	// We need to normalize all this so that all fields are represented and the other field values are null.
	// Give each unique field name an index. We'll use this map later to construct the results and make sure that
	// the response objects have their fields in the result.
	fieldIndexes := make(map[string]int)
	for _, response := range responses {
		for _, name := range response.Series.Fields {
			if _, hasField := fieldIndexes[name]; !hasField {
				fieldIndexes[name] = len(fieldIndexes)
			}
		}
	}
	fields := make([]string, len(fieldIndexes), len(fieldIndexes))
	for name, index := range fieldIndexes {
		fields[index] = name
	}
	fieldCount := len(fields)

	result := &protocol.Series{Name: responses[0].Series.Name, Fields: fields, Points: make([]*protocol.Point, 0)}
	if leftover == nil {
		leftover = &protocol.Series{Name: responses[0].Series.Name, Fields: fields, Points: make([]*protocol.Point, 0)}
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
			// if this is the case we know that all responses contained the same
			// fields. So just append the points
			if len(response.Series.Fields) == fieldCount {
				result.Points = append(result.Points, response.Series.Points...)
			} else {
				log.Debug("Responses from servers had different numbers of fields.")
				for _, p := range response.Series.Points {
					self.normalizePointAndAppend(fieldIndexes, result, response.Series.Fields, p)
				}
			}
		}
		if len(leftover.Fields) == fieldCount {
			result.Points = append(result.Points, leftover.Points...)
			leftover.Points = []*protocol.Point{}
		} else {
			log.Debug("Responses from servers had different numbers of fields.")
			for _, p := range leftover.Points {
				self.normalizePointAndAppend(fieldIndexes, result, leftover.Fields, p)
			}
		}
	} else {
		for _, response := range responses {
			if shouldYieldComparator(response.NextPointTime) {
				// all points safe to yield
				if fieldCount == len(response.Series.Fields) {
					result.Points = append(result.Points, response.Series.Points...)
				} else {
					log.Debug("Responses from servers had different numbers of fields.")
					for _, p := range response.Series.Points {
						self.normalizePointAndAppend(fieldIndexes, result, response.Series.Fields, p)
					}
				}
				continue
			}

			if fieldCount == len(response.Series.Fields) {
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
			} else {
				for i, point := range response.Series.Points {
					if shouldYieldComparator(point.Timestamp) {
						self.normalizePointAndAppend(fieldIndexes, result, response.Series.Fields, point)
					} else {
						// since they're returned in order, we can just append these to
						// the leftover and break out.
						for _, point := range response.Series.Points[i:] {
							self.normalizePointAndAppend(fieldIndexes, leftover, response.Series.Fields, point)
						}
						break
					}
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

func (self *CoordinatorImpl) normalizePointAndAppend(fieldNames map[string]int, result *protocol.Series, fields []string, point *protocol.Point) {
	oldValues := point.Values
	point.Values = make([]*protocol.FieldValue, len(fieldNames), len(fieldNames))
	for index, field := range fields {
		indexForField, ok := fieldNames[field]

		// drop this point on the floor if the unexpected happens
		if !ok {
			log.Error("Couldn't lookup field: ", field, fields, fieldNames)
			return
		}
		point.Values[indexForField] = oldValues[index]
	}
	result.Points = append(result.Points, point)
}

func (self *CoordinatorImpl) SyncLogs() {
	for {
		self.SyncLogIteration()
		time.Sleep(time.Second)
	}
}

func (self *CoordinatorImpl) SyncLogIteration() {
	defer func() {
		if err := recover(); err != nil {
			buf := make([]byte, 1024)
			n := runtime.Stack(buf, false)
			log.Error("recovering from panic in SyncLogIteration: %s. Stacktrace: %s", err, string(buf[:n]))
		}
	}()

	servers := self.clusterConfiguration.Servers()

	replicationFactors := map[uint8]bool{}
	for _, replicationFactor := range self.clusterConfiguration.databaseReplicationFactors {
		replicationFactors[replicationFactor] = true
	}

	localId := self.clusterConfiguration.localServerId

	for replicationFactor, _ := range replicationFactors {
		for _, owningServer := range servers {
		outer:
			for _, originatingServer := range servers {
				if originatingServer.Id == localId {
					continue
				}

				var lastKnownSequenceNumber, currentSequenceNumber uint64
				var err error

				for i := 0; i < 2; i++ {
					if i == 0 {
						lastKnownSequenceNumber, currentSequenceNumber, err = self.getLastAndCurrentSequenceNumbers(replicationFactor, originatingServer, owningServer)
					} else {
						lastKnownSequenceNumber, err = self.GetLastSequenceNumber(replicationFactor, originatingServer.Id, owningServer.Id)
					}

					if err != nil {
						log.Error("Cannot get sequence numbers: %s", err)
						continue
					}

					if lastKnownSequenceNumber >= currentSequenceNumber {
						log.Debug("[%d] Sequence numbers are in sync for originating server %d and owner server %d and replication factor %d [%d %d]",
							localId, originatingServer.Id, owningServer.Id, replicationFactor, lastKnownSequenceNumber, currentSequenceNumber)
						continue outer
					}

					log.Info("[%d] Sequence numbers are out of sync for originating server %d and owner server %d and replication factor %d [%d %d]",
						localId, originatingServer.Id, owningServer.Id, replicationFactor, lastKnownSequenceNumber, currentSequenceNumber)

					// if the sequence numbers are out of sync the first time,
					// wait a second in case there are requests comming in that
					// will increase the last known sequence number
					if i == 0 {
						time.Sleep(time.Second)
					}
				}

				log.Info("[%d] Syncing log for originating server %d and owner server %d and replication factor %d starting at %d",
					localId, originatingServer.Id, owningServer.Id, replicationFactor, lastKnownSequenceNumber)

				request := &protocol.Request{
					OriginatingServerId: &originatingServer.Id,
					ClusterVersion:      &self.clusterConfiguration.ClusterVersion,
				}
				self.ReplayReplication(request, &replicationFactor, &owningServer.Id, &lastKnownSequenceNumber)
			}
		}
	}
}

func (self *CoordinatorImpl) getLastAndCurrentSequenceNumbers(replicationFactor uint8, originatingServer, owningServer *ClusterServer) (uint64, uint64, error) {
	lastKnownSequenceNumber, err := self.GetLastSequenceNumber(replicationFactor, originatingServer.Id, owningServer.Id)
	if err != nil {
		return 0, 0, err
	}

	currentSequenceNumber, err := self.getCurrentSequenceNumber(replicationFactor, originatingServer, owningServer)
	if err != nil {
		return 0, 0, err
	}

	return lastKnownSequenceNumber, currentSequenceNumber, nil
}

func (self *CoordinatorImpl) GetLastSequenceNumber(replicationFactor uint8, originatingServer, owningServer uint32) (uint64, error) {
	return self.datastore.CurrentSequenceNumber(&self.clusterConfiguration.ClusterVersion,
		&replicationFactor, &owningServer, &originatingServer)
}

func (self *CoordinatorImpl) getCurrentSequenceNumber(replicationFactor uint8, originatingServer, owningServer *ClusterServer) (uint64, error) {
	id := atomic.AddUint32(&self.requestId, uint32(1))
	replicationFactor32 := uint32(replicationFactor)
	database := ""
	replayRequest := &protocol.Request{
		Id:                  &id,
		Type:                &sequenceNumber,
		Database:            &database,
		ReplicationFactor:   &replicationFactor32,
		OriginatingServerId: &originatingServer.Id,
		OwnerServerId:       &owningServer.Id,
		ClusterVersion:      &self.clusterConfiguration.ClusterVersion,
	}
	responses := make(chan *protocol.Response)
	err := originatingServer.MakeRequest(replayRequest, responses)
	if err != nil {
		return 0, err
	}
	response := <-responses
	if response == nil {
		return 0, fmt.Errorf("Get a nil response back")
	}
	if response.ErrorCode != nil {
		return 0, fmt.Errorf("Internal server error")
	}
	return *response.Request.LastKnownSequenceNumber, nil
}

func (self *CoordinatorImpl) ReplayReplication(request *protocol.Request, replicationFactor *uint8, owningServerId *uint32, lastSeenSequenceNumber *uint64) {
	log.Warn("COORDINATOR: ReplayReplication: LS: %d, RF: %d, OS: %d", *lastSeenSequenceNumber, *replicationFactor, *owningServerId)
	key := fmt.Sprintf("%d_%d_%d_%d", *replicationFactor, *request.ClusterVersion, *request.OriginatingServerId, *owningServerId)
	self.runningReplaysLock.Lock()
	requestsWaitingToWrite := self.runningReplays[key]
	if requestsWaitingToWrite != nil {
		// request will be nil if this is a forced replay
		if request.Type == nil {
			return
		}
		self.runningReplays[key] = append(requestsWaitingToWrite, request)
		self.runningReplaysLock.Unlock()
		return
	}
	self.runningReplays[key] = []*protocol.Request{}
	if request.Type != nil {
		self.runningReplays[key] = append(self.runningReplays[key], request)
	}
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
	err := server.MakeRequest(replayRequest, replayedRequests)
	if err != nil {
		log.Error("REPLAY ERROR: ", err)
		return
	}
	for {
		response := <-replayedRequests
		if response == nil || *response.Type == protocol.Response_REPLICATION_REPLAY_END {
			self.runningReplaysLock.Lock()
			defer self.runningReplaysLock.Unlock()
			for _, r := range self.runningReplays[key] {
				self.handleReplayRequest(r, replicationFactor, owningServerId)
			}
			delete(self.runningReplays, key)
			log.Info("Replay done for originating server %d and owner server %d", *request.OriginatingServerId, *owningServerId)
			return
		}
		request := response.Request
		log.Debug("Replaying %v", request)
		self.handleReplayRequest(request, replicationFactor, owningServerId)
	}
}

func (self *CoordinatorImpl) handleReplayRequest(r *protocol.Request, replicationFactor *uint8, owningServerId *uint32) {
	err := self.datastore.LogRequestAndAssignSequenceNumber(r, replicationFactor, owningServerId)
	if err != nil {
		log.Error("Error writing waiting requests after replay: %s", err)
	}
	if *r.Type == protocol.Request_PROXY_WRITE || *r.Type == protocol.Request_REPLICATION_WRITE {
		log.Debug("Replaying write request")
		self.datastore.WriteSeriesData(*r.Database, r.Series)
	} else if *r.Type == protocol.Request_PROXY_DELETE || *r.Type == protocol.Request_REPLICATION_DELETE {
		query, _ := parser.ParseQuery(*r.Query)
		err = self.datastore.DeleteSeriesData(*r.Database, query[0].DeleteQuery)
	}
}
func (self *CoordinatorImpl) WriteSeriesData(user common.User, db string, series *protocol.Series) error {
	if !user.HasWriteAccess(db) {
		return common.NewAuthorizationError("Insufficient permission to write to %s", db)
	}
	if len(series.Points) == 0 {
		return fmt.Errorf("Can't write series with zero points.")
	}

	err := self.CommitSeriesData(db, series)

	self.ProcessContinuousQueries(db, series)

	return err
}

func (self *CoordinatorImpl) ProcessContinuousQueries(db string, series *protocol.Series) {
	if self.clusterConfiguration.parsedContinuousQueries != nil {
		incomingSeriesName := *series.Name
		for _, query := range self.clusterConfiguration.parsedContinuousQueries[db] {
			groupByClause := query.GetGroupByClause()
			if groupByClause.Elems != nil {
				continue
			}

			fromClause := query.GetFromClause()
			intoClause := query.GetIntoClause()
			targetName := intoClause.Target.Name

			interpolatedTargetName := strings.Replace(targetName, ":series_name", incomingSeriesName, -1)

			for _, table := range fromClause.Names {
				tableValue := table.Name
				if regex, ok := tableValue.GetCompiledRegex(); ok {
					if regex.MatchString(incomingSeriesName) {
						series.Name = &interpolatedTargetName
						if e := self.CommitSeriesData(db, series); e != nil {
							log.Error("Couldn't write data for continuous query: ", e)
						}
					}
				} else {
					if tableValue.Name == incomingSeriesName {
						series.Name = &interpolatedTargetName
						if e := self.CommitSeriesData(db, series); e != nil {
							log.Error("Couldn't write data for continuous query: ", e)
						}
					}
				}
			}
		}
	}
}

func (self *CoordinatorImpl) CommitSeriesData(db string, series *protocol.Series) error {
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

func (self *CoordinatorImpl) DeleteSeriesData(user common.User, db string, query *parser.DeleteQuery, localOnly bool) error {
	if !user.IsDbAdmin(db) {
		return common.NewAuthorizationError("Insufficient permission to write to %s", db)
	}

	if self.clusterConfiguration.IsSingleServer() || localOnly {
		return self.deleteSeriesDataLocally(user, db, query)
	}

	servers, _ := self.clusterConfiguration.GetServersToMakeQueryTo(&db)
	for _, server := range servers {
		if err := self.handleSeriesDelete(user, server.server, db, query); err != nil {
			return err
		}
	}

	return nil
}

func (self *CoordinatorImpl) deleteSeriesDataLocally(user common.User, database string, query *parser.DeleteQuery) error {
	return self.datastore.DeleteSeriesData(database, query)
}

func (self *CoordinatorImpl) createRequest(requestType protocol.Request_Type, database *string) *protocol.Request {
	id := atomic.AddUint32(&self.requestId, uint32(1))
	return &protocol.Request{Type: &requestType, Database: database, Id: &id}
}

func (self *CoordinatorImpl) handleSeriesDelete(user common.User, server *ClusterServer, database string, query *parser.DeleteQuery) error {
	owner, servers := self.clusterConfiguration.GetReplicas(server, &database)

	request := self.createRequest(proxyDelete, &database)
	queryStr := query.GetQueryStringWithTimeCondition()
	request.Query = &queryStr
	request.OriginatingServerId = &self.clusterConfiguration.localServerId
	request.ClusterVersion = &self.clusterConfiguration.ClusterVersion
	request.OwnerServerId = &owner.Id

	if server.Id == self.clusterConfiguration.localServerId {
		// this is a local delete
		replicationFactor := self.clusterConfiguration.GetReplicationFactor(&database)
		err := self.datastore.LogRequestAndAssignSequenceNumber(request, &replicationFactor, &owner.Id)
		if err != nil {
			return self.proxyUntilSuccess(servers, request)
		}
		self.deleteSeriesDataLocally(user, database, query)
		if err != nil {
			log.Error("Couldn't write data to local store: ", err, request)
		}

		// ignoring the error because we still want to send to replicas
		request.Type = &replicateDelete
		self.sendRequestToReplicas(request, servers)
		return nil
	}

	// otherwise, proxy the delete
	return self.proxyUntilSuccess(servers, request)
}

func (self *CoordinatorImpl) handleDropDatabase(server *ClusterServer, database string) error {
	owner, servers := self.clusterConfiguration.GetReplicas(server, &database)

	request := self.createRequest(proxyDropDatabase, &database)
	request.OriginatingServerId = &self.clusterConfiguration.localServerId
	request.ClusterVersion = &self.clusterConfiguration.ClusterVersion
	request.OwnerServerId = &owner.Id
	replicationFactor := uint32(self.clusterConfiguration.GetDatabaseReplicationFactor(database))
	request.ReplicationFactor = &replicationFactor

	if server.Id == self.clusterConfiguration.localServerId {
		// this is a local delete
		replicationFactor := self.clusterConfiguration.GetReplicationFactor(&database)
		err := self.datastore.LogRequestAndAssignSequenceNumber(request, &replicationFactor, &owner.Id)
		if err != nil {
			return self.proxyUntilSuccess(servers, request)
		}
		self.datastore.DropDatabase(database)
		if err != nil {
			log.Error("Couldn't write data to local store: ", err, request)
		}

		// ignoring the error because we still want to send to replicas
		request.Type = &replicateDropDatabase
		self.sendRequestToReplicas(request, servers)
		return nil
	}

	// otherwise, proxy the request
	return self.proxyUntilSuccess(servers, request)
}

func (self *CoordinatorImpl) handleDropSeries(server *ClusterServer, database, series string) error {
	owner, servers := self.clusterConfiguration.GetReplicas(server, &database)

	request := self.createRequest(proxyDropSeries, &database)
	request.OriginatingServerId = &self.clusterConfiguration.localServerId
	request.ClusterVersion = &self.clusterConfiguration.ClusterVersion
	request.OwnerServerId = &owner.Id
	request.Series = &protocol.Series{Name: &series}
	replicationFactor := uint32(self.clusterConfiguration.GetDatabaseReplicationFactor(database))
	request.ReplicationFactor = &replicationFactor

	if server.Id == self.clusterConfiguration.localServerId {
		// this is a local delete
		replicationFactor := self.clusterConfiguration.GetReplicationFactor(&database)
		err := self.datastore.LogRequestAndAssignSequenceNumber(request, &replicationFactor, &owner.Id)
		if err != nil {
			return self.proxyUntilSuccess(servers, request)
		}
		self.datastore.DropSeries(database, series)
		if err != nil {
			log.Error("Couldn't write data to local store: ", err, request)
		}

		// ignoring the error because we still want to send to replicas
		request.Type = &replicateDropSeries
		self.sendRequestToReplicas(request, servers)
		return nil
	}

	// otherwise, proxy the request
	return self.proxyUntilSuccess(servers, request)
}

func (self *CoordinatorImpl) writeSeriesToLocalStore(db *string, series *protocol.Series) error {
	return self.datastore.WriteSeriesData(*db, series)
}

func (self *CoordinatorImpl) handleClusterWrite(serverIndex *int, db *string, series *protocol.Series) error {
	// TODO: Figure out how to not need this lock. Shouldn't have to lock to send on a connection. However,
	// when the server is under load, replication requests can get out of order, which triggers a replay.
	// Maybe we need a special channel for replication?
	self.writeLock.Lock()
	defer self.writeLock.Unlock()

	owner, servers := self.clusterConfiguration.GetServersByIndexAndReplicationFactor(db, serverIndex)

	request := self.createRequest(proxyWrite, db)
	request.Series = series
	request.OriginatingServerId = &self.clusterConfiguration.localServerId
	request.ClusterVersion = &self.clusterConfiguration.ClusterVersion
	request.OwnerServerId = &owner.Id

	for _, s := range servers {
		if s.Id == self.clusterConfiguration.localServerId {
			// TODO: make storing of the data and logging of the request atomic
			replicationFactor := self.clusterConfiguration.GetReplicationFactor(db)
			err := self.datastore.LogRequestAndAssignSequenceNumber(request, &replicationFactor, &owner.Id)
			if err != nil {
				return self.proxyUntilSuccess(servers, request)
			}

			// ignoring the error for writing to the local store because we still want to send to replicas
			err = self.writeSeriesToLocalStore(db, series)
			if err != nil {
				log.Error("Couldn't write data to local store: ", err, request)
			}
			request.Type = &replicateWrite
			self.sendRequestToReplicas(request, servers)

			return nil
		}
	}

	// it didn't live locally so proxy it
	return self.proxyUntilSuccess(servers, request)
}

// This method will attemp to proxy the request until the call to proxy returns nil. If no server succeeds,
// the last err value will be returned.
func (self *CoordinatorImpl) proxyUntilSuccess(servers []*ClusterServer, request *protocol.Request) (err error) {
	for _, s := range servers {
		if s.Id != self.clusterConfiguration.localServerId {
			err = self.proxyWrite(s, request)
			if err == nil {
				return nil
			}
		}
	}
	return
}

func (self *CoordinatorImpl) proxyWrite(clusterServer *ClusterServer, request *protocol.Request) error {
	originatingServerId := request.OriginatingServerId
	request.OriginatingServerId = nil
	defer func() { request.OriginatingServerId = originatingServerId }()

	responseChan := make(chan *protocol.Response, 1)
	err := clusterServer.MakeRequest(request, responseChan)
	if err != nil {
		log.Warn("PROXY WRITE ERROR: ", err)
		return err
	}
	response := <-responseChan
	if *response.Type == protocol.Response_WRITE_OK {
		return nil
	} else {
		return errors.New(response.GetErrorMessage())
	}
}

func (self *CoordinatorImpl) CreateContinuousQuery(user common.User, db string, query string) error {
	if !user.IsClusterAdmin() && !user.IsDbAdmin(db) {
		return common.NewAuthorizationError("Insufficient permission to create continuous query")
	}

	err := self.raftServer.CreateContinuousQuery(db, query)
	if err != nil {
		return err
	}
	return nil
}

func (self *CoordinatorImpl) DeleteContinuousQuery(user common.User, db string, id uint32) error {
	if !user.IsClusterAdmin() && !user.IsDbAdmin(db) {
		return common.NewAuthorizationError("Insufficient permission to delete continuous query")
	}

	err := self.raftServer.DeleteContinuousQuery(db, id)
	if err != nil {
		return err
	}
	return nil
}

func (self *CoordinatorImpl) ListContinuousQueries(user common.User, db string) ([]*protocol.Series, error) {
	if !user.IsClusterAdmin() && !user.IsDbAdmin(db) {
		return nil, common.NewAuthorizationError("Insufficient permission to list continuous queries")
	}

	queries := self.clusterConfiguration.GetContinuousQueries(db)
	points := []*protocol.Point{}

	for _, query := range queries {
		queryId := int64(query.Id)
		queryString := query.Query
		timestamp := time.Now().Unix()
		sequenceNumber := uint64(1)
		points = append(points, &protocol.Point{
			Values: []*protocol.FieldValue{
				&protocol.FieldValue{Int64Value: &queryId},
				&protocol.FieldValue{StringValue: &queryString},
			},
			Timestamp:      &timestamp,
			SequenceNumber: &sequenceNumber,
		})
	}
	seriesName := "continuous queries"
	series := []*protocol.Series{&protocol.Series{
		Name:   &seriesName,
		Fields: []string{"id", "query"},
		Points: points,
	}}
	return series, nil
}

func (self *CoordinatorImpl) CreateDatabase(user common.User, db string, replicationFactor uint8) error {
	if !user.IsClusterAdmin() {
		return common.NewAuthorizationError("Insufficient permission to create database")
	}

	if !isValidName(db) {
		return fmt.Errorf("%s isn't a valid db name", db)
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

func seriesFromListSeries(names []string) []*protocol.Series {
	series := []*protocol.Series{}

	for _, _name := range names {
		name := _name
		series = append(series, &protocol.Series{
			Name:   &name,
			Fields: []string{},
			Points: []*protocol.Point{},
		})
	}
	return series
}

func (self *CoordinatorImpl) ListSeries(user common.User, database string) ([]*protocol.Series, error) {
	if self.clusterConfiguration.IsSingleServer() {
		dbs := []string{}
		self.datastore.GetSeriesForDatabase(database, func(db string) error {
			dbs = append(dbs, db)
			return nil
		})
		return seriesFromListSeries(dbs), nil
	}
	servers, replicationFactor := self.clusterConfiguration.GetServersToMakeQueryTo(&database)
	id := atomic.AddUint32(&self.requestId, uint32(1))
	userName := user.GetName()
	isDbUser := !user.IsClusterAdmin()
	responseChannels := make([]chan *protocol.Response, 0, len(servers)+1)
	for _, server := range servers {
		if server.server.Id == self.clusterConfiguration.localServerId {
			continue
		}
		request := &protocol.Request{Type: &listSeriesRequest, Id: &id, Database: &database, UserName: &userName, IsDbUser: &isDbUser}
		if server.ringLocationsToQuery != replicationFactor {
			r := server.ringLocationsToQuery
			request.RingLocationsToQuery = &r
		}
		responseChan := make(chan *protocol.Response, 3)
		server.server.protobufClient.MakeRequest(request, responseChan)
		responseChannels = append(responseChannels, responseChan)
	}

	local := make(chan *protocol.Response)

	responseChannels = append(responseChannels, local)

	go func() {
		dbs := []string{}
		self.datastore.GetSeriesForDatabase(database, func(db string) error {
			dbs = append(dbs, db)
			return nil
		})
		seriesArray := seriesFromListSeries(dbs)
		for _, series := range seriesArray {
			local <- &protocol.Response{Type: &listSeriesResponse, Series: series}
		}
		local <- &protocol.Response{Type: &endStreamResponse}
		close(local)
	}()
	seriesArray := []*protocol.Series{}
	self.streamResultsFromChannels(true, true, responseChannels, func(series *protocol.Series) error {
		seriesArray = append(seriesArray, series)
		return nil
	})
	return seriesArray, nil
}

func (self *CoordinatorImpl) DropDatabase(user common.User, db string) error {
	if !user.IsClusterAdmin() {
		return common.NewAuthorizationError("Insufficient permission to drop database")
	}

	if self.clusterConfiguration.IsSingleServer() {
		if err := self.datastore.DropDatabase(db); err != nil {
			return err
		}
	} else {
		servers, _ := self.clusterConfiguration.GetServersToMakeQueryTo(&db)
		for _, server := range servers {
			if err := self.handleDropDatabase(server.server, db); err != nil {
				return err
			}
		}
	}

	// don't delete the metadata, we need the replication factor to be
	// able to replicate the request properly
	if err := self.raftServer.DropDatabase(db); err != nil {
		return err
	}

	return nil
}

func (self *CoordinatorImpl) DropSeries(user common.User, db, series string) error {
	if !user.IsClusterAdmin() && !user.IsDbAdmin(db) && !user.HasWriteAccess(series) {
		return common.NewAuthorizationError("Insufficient permission to drop series")
	}

	if self.clusterConfiguration.IsSingleServer() {
		return self.datastore.DropSeries(db, series)
	}

	servers, _ := self.clusterConfiguration.GetServersToMakeQueryTo(&db)
	for _, server := range servers {
		if err := self.handleDropSeries(server.server, db, series); err != nil {
			return err
		}
	}

	return nil
}

func (self *CoordinatorImpl) AuthenticateDbUser(db, username, password string) (common.User, error) {
	log.Debug("(raft:%s) Authenticating password for %s:%s", self.raftServer.(*RaftServer).raftServer.Name(), db, username)
	dbUsers := self.clusterConfiguration.dbUsers[db]
	if dbUsers == nil || dbUsers[username] == nil {
		return nil, common.NewAuthorizationError("Invalid username/password")
	}
	user := dbUsers[username]
	if user.isValidPwd(password) {
		log.Debug("(raft:%s) User %s authenticated succesfuly", self.raftServer.(*RaftServer).raftServer.Name(), username)
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

	if !isValidName(username) {
		return fmt.Errorf("%s isn't a valid username", username)
	}

	if self.clusterConfiguration.clusterAdmins[username] != nil {
		return fmt.Errorf("User %s already exists", username)
	}

	return self.raftServer.SaveClusterAdminUser(&clusterAdmin{CommonUser{Name: username, CacheKey: username}})
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

	hash, err := hashPassword(password)
	if err != nil {
		return err
	}
	user.changePassword(string(hash))
	return self.raftServer.SaveClusterAdminUser(user)
}

func (self *CoordinatorImpl) CreateDbUser(requester common.User, db, username string) error {
	if !requester.IsClusterAdmin() && !requester.IsDbAdmin(db) {
		return common.NewAuthorizationError("Insufficient permissions")
	}

	if username == "" {
		return fmt.Errorf("Username cannot be empty")
	}

	if !isValidName(username) {
		return fmt.Errorf("%s isn't a valid username", username)
	}

	self.CreateDatabase(requester, db, uint8(1)) // ignore the error since the db may exist
	dbUsers := self.clusterConfiguration.dbUsers[db]
	if dbUsers != nil && dbUsers[username] != nil {
		return fmt.Errorf("User %s already exists", username)
	}
	matchers := []*Matcher{&Matcher{true, ".*"}}
	log.Debug("(raft:%s) Creating uesr %s:%s", self.raftServer.(*RaftServer).raftServer.Name(), db, username)
	return self.raftServer.SaveDbUser(&dbUser{CommonUser{Name: username, CacheKey: db + "%" + username}, db, matchers, matchers, false})
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

	hash, err := hashPassword(password)
	if err != nil {
		return err
	}
	return self.raftServer.ChangeDbUserPassword(db, username, hash)
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
	log.Info("Waiting for local server to be added")
	self.clusterConfiguration.WaitForLocalServerLoaded()
	log.Info("Local server added. Connecting to other nodes in the cluster")

	for _, server := range self.clusterConfiguration.Servers() {
		if server.ProtobufConnectionString != localConnectionString {
			server.Connect()
		}
	}
	return nil
}

func (self *CoordinatorImpl) ReplicateWrite(request *protocol.Request) error {
	id := atomic.AddUint32(&self.requestId, uint32(1))
	request.Id = &id
	location := common.RingLocation(request.Database, request.Series.Name, request.Series.Points[0].Timestamp)
	replicas := self.clusterConfiguration.GetServersByRingLocation(request.Database, &location)
	request.Type = &replicateWrite
	self.sendRequestToReplicas(request, replicas)
	return nil
}

func (self *CoordinatorImpl) ReplicateDelete(request *protocol.Request) error {
	id := atomic.AddUint32(&self.requestId, uint32(1))
	request.Id = &id
	server := self.clusterConfiguration.GetServerById(request.OwnerServerId)
	_, replicas := self.clusterConfiguration.GetReplicas(server, request.Database)
	request.Type = &replicateDelete
	self.sendRequestToReplicas(request, replicas)
	return nil
}

func (self *CoordinatorImpl) sendRequestToReplicas(request *protocol.Request, replicas []*ClusterServer) {
	for _, server := range replicas {
		if server.Id != self.clusterConfiguration.localServerId {
			err := server.MakeRequest(request, nil)
			if err != nil {
				log.Warn("REPLICATION ERROR: ", request.GetSequenceNumber(), err)
			}
		}
	}
}

func (self *CoordinatorImpl) sequenceNumberWithServerId(n uint64) uint64 {
	return n*HOST_ID_OFFSET + uint64(self.clusterConfiguration.localServerId)
}

func isValidName(name string) bool {
	return VALID_NAMES.MatchString(name)
}
