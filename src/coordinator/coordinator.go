package coordinator

import (
	"cluster"
	log "code.google.com/p/log4go"
	"common"
	"datastore"
	"engine"
	"fmt"
	"math"
	"os"
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
	clusterConfiguration *cluster.ClusterConfiguration
	raftServer           ClusterConsensus
	datastore            datastore.Datastore
	requestId            uint32
	runningReplays       map[string][]*protocol.Request
	runningReplaysLock   sync.Mutex
	writeLock            sync.Mutex
}

const (
	// this is the key used for the persistent atomic ints for sequence numbers
	POINT_SEQUENCE_NUMBER_KEY = "p"

	// actual point sequence numbers will have the first part of the number
	// be a host id. This ensures that sequence numbers are unique across the cluster
	HOST_ID_OFFSET = uint64(10000)

	SHARDS_TO_QUERY_FOR_LIST_SERIES = 10
)

var (
	BARRIER_TIME_MIN int64 = math.MinInt64
	BARRIER_TIME_MAX int64 = math.MaxInt64
)

// shorter constants for readability
var (
	dropDatabase      = protocol.Request_DROP_DATABASE
	queryRequest      = protocol.Request_QUERY
	endStreamResponse = protocol.Response_END_STREAM
	queryResponse     = protocol.Response_QUERY
	replayReplication = protocol.Request_REPLICATION_REPLAY
	sequenceNumber    = protocol.Request_SEQUENCE_NUMBER

	write = protocol.Request_WRITE
)

type SeriesWriter interface {
	Write(*protocol.Series) error
	Close()
}

// usernames and db names should match this regex
var VALID_NAMES *regexp.Regexp

func init() {
	var err error
	VALID_NAMES, err = regexp.Compile("^[a-zA-Z0-9_][a-zA-Z0-9\\._-]*$")
	if err != nil {
		panic(err)
	}
}

func NewCoordinatorImpl(datastore datastore.Datastore, raftServer ClusterConsensus, clusterConfiguration *cluster.ClusterConfiguration) *CoordinatorImpl {
	coordinator := &CoordinatorImpl{
		clusterConfiguration: clusterConfiguration,
		raftServer:           raftServer,
		datastore:            datastore,
		runningReplays:       make(map[string][]*protocol.Request),
	}

	return coordinator
}

func (self *CoordinatorImpl) RunQuery(user common.User, database string, queryString string, seriesWriter SeriesWriter) (err error) {
	// don't let a panic pass beyond RunQuery
	defer recoverFunc(database, queryString)

	q, err := parser.ParseQuery(queryString)
	if err != nil {
		return err
	}

	for _, query := range q {
		querySpec := parser.NewQuerySpec(user, database, query)

		if query.DeleteQuery != nil {
			if err := self.runDeleteQuery(querySpec, seriesWriter); err != nil {
				return err
			}
			continue
		}

		if query.DropQuery != nil {
			if err := self.DeleteContinuousQuery(user, database, uint32(query.DropQuery.Id)); err != nil {
				return err
			}
			continue
		}

		if query.IsListQuery() {
			if query.IsListSeriesQuery() {
				self.runListSeriesQuery(querySpec, seriesWriter)
			} else if query.IsListContinuousQueriesQuery() {
				queries, err := self.ListContinuousQueries(user, database)
				if err != nil {
					return err
				}
				for _, q := range queries {
					if err := seriesWriter.Write(q); err != nil {
						return err
					}
				}
			}
			continue
		}

		if query.DropSeriesQuery != nil {
			err := self.runDropSeriesQuery(querySpec, seriesWriter)
			if err != nil {
				return err
			}
			continue
		}

		selectQuery := query.SelectQuery

		if selectQuery.IsContinuousQuery() {
			return self.CreateContinuousQuery(user, database, queryString)
		}

		return self.runQuery(query, user, database, seriesWriter)
	}
	seriesWriter.Close()
	return nil
}

// This should only get run for SelectQuery types
func (self *CoordinatorImpl) runQuery(query *parser.Query, user common.User, database string, seriesWriter SeriesWriter) error {
	querySpec := parser.NewQuerySpec(user, database, query)
	return self.runQuerySpec(querySpec, seriesWriter)
}

func (self *CoordinatorImpl) runListSeriesQuery(querySpec *parser.QuerySpec, seriesWriter SeriesWriter) error {
	shortTermShards := self.clusterConfiguration.GetShortTermShards()
	if len(shortTermShards) > SHARDS_TO_QUERY_FOR_LIST_SERIES {
		shortTermShards = shortTermShards[:SHARDS_TO_QUERY_FOR_LIST_SERIES]
	}
	longTermShards := self.clusterConfiguration.GetLongTermShards()
	if len(longTermShards) > SHARDS_TO_QUERY_FOR_LIST_SERIES {
		longTermShards = longTermShards[:SHARDS_TO_QUERY_FOR_LIST_SERIES]
	}
	seriesYielded := make(map[string]bool)

	responses := make([]chan *protocol.Response, 0)
	for _, shard := range shortTermShards {
		responseChan := make(chan *protocol.Response, 1)
		go shard.Query(querySpec, responseChan)
		responses = append(responses, responseChan)
	}
	for _, shard := range longTermShards {
		responseChan := make(chan *protocol.Response, 1)
		go shard.Query(querySpec, responseChan)
		responses = append(responses, responseChan)
	}

	for _, responseChan := range responses {
		for {
			response := <-responseChan
			if *response.Type == endStreamResponse {
				break
			}
			for _, series := range response.MultiSeries {
				if !seriesYielded[*series.Name] {
					seriesYielded[*series.Name] = true
					seriesWriter.Write(series)
				}
			}
		}
	}
	return nil
}

func (self *CoordinatorImpl) runDeleteQuery(querySpec *parser.QuerySpec, seriesWriter SeriesWriter) error {
	db := querySpec.Database()
	if !querySpec.User().IsDbAdmin(db) {
		return common.NewAuthorizationError("Insufficient permission to write to %s", db)
	}
	querySpec.RunAgainstAllServersInShard = true
	return self.runQuerySpec(querySpec, seriesWriter)
}

func (self *CoordinatorImpl) runDropSeriesQuery(querySpec *parser.QuerySpec, seriesWriter SeriesWriter) error {
	user := querySpec.User()
	db := querySpec.Database()
	series := querySpec.Query().DropSeriesQuery.GetTableName()
	if !user.IsClusterAdmin() && !user.IsDbAdmin(db) && !user.HasWriteAccess(series) {
		return common.NewAuthorizationError("Insufficient permission to drop series")
	}
	querySpec.RunAgainstAllServersInShard = true
	return self.runQuerySpec(querySpec, seriesWriter)
}

func (self *CoordinatorImpl) runQuerySpec(querySpec *parser.QuerySpec, seriesWriter SeriesWriter) error {
	shards := self.clusterConfiguration.GetShards(querySpec)

	shouldAggregateLocally := true
	var processor cluster.QueryProcessor
	var responseChan chan *protocol.Response
	for _, s := range shards {
		// If the aggregation is done at the shard level, we don't need to
		// do it here at the coordinator level.
		if !s.ShouldAggregateLocally(querySpec) {
			shouldAggregateLocally = false
			responseChan = make(chan *protocol.Response)
			processor = engine.NewQueryEngine(querySpec.SelectQuery(), responseChan)
			go func() {
				for {
					res := <-responseChan
					if *res.Type == endStreamResponse {
						return
					}
					seriesWriter.Write(res.Series)
				}
			}()
			break
		}
	}

	responses := make([]chan *protocol.Response, 0)
	for _, shard := range shards {
		responseChan := make(chan *protocol.Response, 1)
		go shard.Query(querySpec, responseChan)
		responses = append(responses, responseChan)
	}

	for _, responseChan := range responses {
		for {
			response := <-responseChan
			if *response.Type == endStreamResponse {
				break
			}
			if shouldAggregateLocally {
				seriesWriter.Write(response.Series)
				continue
			}

			// if the data wasn't aggregated at the shard level, aggregate
			// the data here
			if response.Series != nil {
				for _, p := range response.Series.Points {
					processor.YieldPoint(response.Series.Name, response.Series.Fields, p)
				}
			}
		}
	}
	if !shouldAggregateLocally {
		processor.Close()
	}
	seriesWriter.Close()
	return nil
}

func recoverFunc(database, query string) {
	if err := recover(); err != nil {
		fmt.Fprintf(os.Stderr, "********************************BUG********************************\n")
		buf := make([]byte, 1024)
		n := runtime.Stack(buf, false)
		fmt.Fprintf(os.Stderr, "Database: %s\n", database)
		fmt.Fprintf(os.Stderr, "Query: [%s]\n", query)
		fmt.Fprintf(os.Stderr, "Error: %s. Stacktrace: %s\n", err, string(buf[:n]))
		err = common.NewQueryError(common.InternalError, "Internal Error")
	}
}

func (self *CoordinatorImpl) ForceCompaction(user common.User) error {
	if !user.IsClusterAdmin() {
		return fmt.Errorf("Insufficient permission to force a log compaction")
	}

	return self.raftServer.ForceLogCompaction()
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
	if self.clusterConfiguration.ParsedContinuousQueries != nil {
		incomingSeriesName := *series.Name
		for _, query := range self.clusterConfiguration.ParsedContinuousQueries[db] {
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
	lastTime := int64(0)
	lastPointIndex := 0
	now := common.CurrentTime()
	var shardToWrite cluster.Shard
	for i, point := range series.Points {
		if point.Timestamp == nil {
			point.Timestamp = &now
		}
		if *point.Timestamp != lastTime {
			shard, err := self.clusterConfiguration.GetShardToWriteToBySeriesAndTime(db, *series.Name, *point.Timestamp)
			if err != nil {
				return err
			}
			if shardToWrite == nil {
				shardToWrite = shard
			} else if shardToWrite.Id() != shard.Id() {
				newIndex := i + 1
				newSeries := &protocol.Series{Name: series.Name, Fields: series.Fields, Points: series.Points[lastPointIndex:newIndex]}
				self.write(db, newSeries, shard)
				lastPointIndex = newIndex
			}
			lastTime = *point.Timestamp
		}
	}

	err := self.write(db, series, shardToWrite)
	if err != nil {
		log.Error("COORD error writing: ", err)
	}

	return err
}

func (self *CoordinatorImpl) write(db string, series *protocol.Series, shard cluster.Shard) error {
	request := &protocol.Request{Type: &write, Database: &db, Series: series}
	return shard.Write(request)
}

func (self *CoordinatorImpl) createRequest(requestType protocol.Request_Type, database *string) *protocol.Request {
	id := atomic.AddUint32(&self.requestId, uint32(1))
	return &protocol.Request{Type: &requestType, Database: database, Id: &id}
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

func (self *CoordinatorImpl) ListDatabases(user common.User) ([]*cluster.Database, error) {
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

	var wait sync.WaitGroup
	for _, shard := range self.clusterConfiguration.GetAllShards() {
		wait.Add(1)
		go func(shard *cluster.ShardData) {
			shard.DropDatabase(db, true)
			wait.Done()
		}(shard)
	}
	wait.Wait()
	return nil
}

func (self *CoordinatorImpl) AuthenticateDbUser(db, username, password string) (common.User, error) {
	log.Debug("(raft:%s) Authenticating password for %s:%s", self.raftServer.(*RaftServer).raftServer.Name(), db, username)
	user, err := self.clusterConfiguration.AuthenticateDbUser(db, username, password)
	if user != nil {
		log.Debug("(raft:%s) User %s authenticated succesfuly", self.raftServer.(*RaftServer).raftServer.Name(), username)
	}
	return user, err
}

func (self *CoordinatorImpl) AuthenticateClusterAdmin(username, password string) (common.User, error) {
	return self.clusterConfiguration.AuthenticateClusterAdmin(username, password)
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

	if self.clusterConfiguration.GetClusterAdmin(username) != nil {
		return fmt.Errorf("User %s already exists", username)
	}

	return self.raftServer.SaveClusterAdminUser(&cluster.ClusterAdmin{cluster.CommonUser{Name: username, CacheKey: username}})
}

func (self *CoordinatorImpl) DeleteClusterAdminUser(requester common.User, username string) error {
	if !requester.IsClusterAdmin() {
		return common.NewAuthorizationError("Insufficient permissions")
	}

	user := self.clusterConfiguration.GetClusterAdmin(username)
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

	user := self.clusterConfiguration.GetClusterAdmin(username)
	if user == nil {
		return fmt.Errorf("Invalid user name %s", username)
	}

	hash, err := cluster.HashPassword(password)
	if err != nil {
		return err
	}
	user.ChangePassword(string(hash))
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
	if self.clusterConfiguration.GetDbUser(db, username) != nil {
		return fmt.Errorf("User %s already exists", username)
	}
	matchers := []*cluster.Matcher{&cluster.Matcher{true, ".*"}}
	log.Debug("(raft:%s) Creating uesr %s:%s", self.raftServer.(*RaftServer).raftServer.Name(), db, username)
	return self.raftServer.SaveDbUser(&cluster.DbUser{cluster.CommonUser{Name: username, CacheKey: db + "%" + username}, db, matchers, matchers, false})
}

func (self *CoordinatorImpl) DeleteDbUser(requester common.User, db, username string) error {
	if !requester.IsClusterAdmin() && !requester.IsDbAdmin(db) {
		return common.NewAuthorizationError("Insufficient permissions")
	}

	user := self.clusterConfiguration.GetDbUser(db, username)
	if user == nil {
		return fmt.Errorf("User %s doesn't exists", username)
	}
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

	hash, err := cluster.HashPassword(password)
	if err != nil {
		return err
	}
	return self.raftServer.ChangeDbUserPassword(db, username, hash)
}

func (self *CoordinatorImpl) SetDbAdmin(requester common.User, db, username string, isAdmin bool) error {
	if !requester.IsClusterAdmin() && !requester.IsDbAdmin(db) {
		return common.NewAuthorizationError("Insufficient permissions")
	}

	user := self.clusterConfiguration.GetDbUser(db, username)
	if user == nil {
		return fmt.Errorf("Invalid username %s", username)
	}
	user.IsAdmin = isAdmin
	self.raftServer.SaveDbUser(user)
	return nil
}

func (self *CoordinatorImpl) ConnectToProtobufServers(localConnectionString string) error {
	log.Info("Connecting to other nodes in the cluster")

	for _, server := range self.clusterConfiguration.Servers() {
		if server.ProtobufConnectionString != localConnectionString {
			server.Connect()
		}
	}
	return nil
}

func isValidName(name string) bool {
	return VALID_NAMES.MatchString(name)
}
