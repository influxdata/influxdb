package coordinator

import (
	"cluster"
	"common"
	"configuration"
	"engine"
	"fmt"
	"math"
	"parser"
	"protocol"
	"regexp"
	"strings"
	"sync"
	"time"

	log "code.google.com/p/log4go"
)

type CoordinatorImpl struct {
	clusterConfiguration *cluster.ClusterConfiguration
	raftServer           ClusterConsensus
	config               *configuration.Configuration
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
	dropDatabase         = protocol.Request_DROP_DATABASE
	queryRequest         = protocol.Request_QUERY
	endStreamResponse    = protocol.Response_END_STREAM
	queryResponse        = protocol.Response_QUERY
	heartbeatResponse    = protocol.Response_HEARTBEAT
	explainQueryResponse = protocol.Response_EXPLAIN_QUERY
	write                = protocol.Request_WRITE
)

type SeriesWriter interface {
	Write(*protocol.Series) error
	Close()
}

func NewCoordinatorImpl(config *configuration.Configuration, raftServer ClusterConsensus, clusterConfiguration *cluster.ClusterConfiguration) *CoordinatorImpl {
	coordinator := &CoordinatorImpl{
		config:               config,
		clusterConfiguration: clusterConfiguration,
		raftServer:           raftServer,
	}

	return coordinator
}

func (self *CoordinatorImpl) RunQuery(user common.User, database string, queryString string, seriesWriter SeriesWriter) (err error) {
	log.Info("Query: db: %s, u: %s, q: %s", database, user.GetName(), queryString)
	// don't let a panic pass beyond RunQuery
	defer common.RecoverFunc(database, queryString, nil)

	q, err := parser.ParseQuery(queryString)
	if err != nil {
		return err
	}

	for _, query := range q {
		querySpec := parser.NewQuerySpec(user, database, query)

		if query.DeleteQuery != nil {
			if err := self.clusterConfiguration.CreateCheckpoint(); err != nil {
				return err
			}

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
		if err := self.checkPermission(user, querySpec); err != nil {
			return err
		}
		return self.runQuery(querySpec, seriesWriter)
	}
	seriesWriter.Close()
	return nil
}

func (self *CoordinatorImpl) checkPermission(user common.User, querySpec *parser.QuerySpec) error {
	// if this isn't a regex query do the permission check here
	fromClause := querySpec.SelectQuery().GetFromClause()

	for _, n := range fromClause.Names {
		if _, ok := n.Name.GetCompiledRegex(); ok {
			break
		} else if name := n.Name.Name; !user.HasReadAccess(name) {
			return fmt.Errorf("User doesn't have read access to %s", name)
		}
	}
	return nil
}

// This should only get run for SelectQuery types
func (self *CoordinatorImpl) runQuery(querySpec *parser.QuerySpec, seriesWriter SeriesWriter) error {
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

	shards := append(shortTermShards, longTermShards...)

	var err error
	for _, shard := range shards {
		responseChan := make(chan *protocol.Response, shard.QueryResponseBufferSize(querySpec, self.config.LevelDbPointBatchSize))
		go shard.Query(querySpec, responseChan)
		for {
			response := <-responseChan
			if *response.Type == endStreamResponse || *response.Type == accessDeniedResponse {
				if response.ErrorMessage != nil && err != nil {
					log.Debug("Error when querying shard: %s", err)
					err = common.NewQueryError(common.InvalidArgument, *response.ErrorMessage)
				}
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
	seriesWriter.Close()
	return err
}

func (self *CoordinatorImpl) runDeleteQuery(querySpec *parser.QuerySpec, seriesWriter SeriesWriter) error {
	user := querySpec.User()
	db := querySpec.Database()
	if !user.IsClusterAdmin() && !user.IsDbAdmin(db) {
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
		return common.NewAuthorizationError("Insufficient permissions to drop series")
	}
	querySpec.RunAgainstAllServersInShard = true
	return self.runQuerySpec(querySpec, seriesWriter)
}

func (self *CoordinatorImpl) shouldAggregateLocally(shards []*cluster.ShardData, querySpec *parser.QuerySpec) bool {
	for _, s := range shards {
		if !s.ShouldAggregateLocally(querySpec) {
			return false
		}
	}
	return true
}

func (self *CoordinatorImpl) shouldQuerySequentially(shards []*cluster.ShardData, querySpec *parser.QuerySpec) bool {
	// if the query isn't a select, then it doesn't matter
	if querySpec.SelectQuery() == nil {
		return false
	}

	// if the query is a regex, we can't predic the number of responses
	// we get back
	if querySpec.IsRegex() {
		return true
	}
	groupByClause := querySpec.SelectQuery().GetGroupByClause()
	// if there's no group by clause, then we're returning raw points
	// with some math done on them, thus we can't predict the number of
	// points
	if groupByClause == nil {
		return true
	}
	// if there's a group by clause but no group by interval, we can't
	// predict the cardinality of the columns used in the group by
	// interval, thus we can't predict the number of responses returned
	// from the shard
	if querySpec.GetGroupByInterval() == nil {
		return true
	}
	// if there's a group by time and other columns, then the previous
	// logic holds
	if len(groupByClause.Elems) > 1 {
		return true
	}

	if !self.shouldAggregateLocally(shards, querySpec) {
		return true
	}

	for _, shard := range shards {
		bufferSize := shard.QueryResponseBufferSize(querySpec, self.config.LevelDbPointBatchSize)
		// if the number of repsonses is too big, do a sequential querying
		if bufferSize > self.config.ClusterMaxResponseBufferSize {
			return true
		}
	}

	// parallel querying only if we're querying a single series, with
	// group by time only
	return false
}

func (self *CoordinatorImpl) getShardsAndProcessor(querySpec *parser.QuerySpec, writer SeriesWriter) ([]*cluster.ShardData, cluster.QueryProcessor, chan bool, error) {
	shards := self.clusterConfiguration.GetShards(querySpec)
	shouldAggregateLocally := self.shouldAggregateLocally(shards, querySpec)

	var err error
	var processor cluster.QueryProcessor

	responseChan := make(chan *protocol.Response)
	seriesClosed := make(chan bool)

	selectQuery := querySpec.SelectQuery()
	if selectQuery != nil {
		if !shouldAggregateLocally {
			// if we should aggregate in the coordinator (i.e. aggregation
			// isn't happening locally at the shard level), create an engine
			processor, err = engine.NewQueryEngine(querySpec.SelectQuery(), responseChan)
		} else {
			// if we have a query with limit, then create an engine, or we can
			// make the passthrough limit aware
			processor = engine.NewPassthroughEngineWithLimit(responseChan, 100, selectQuery.Limit)
		}
	} else if !shouldAggregateLocally {
		processor = engine.NewPassthroughEngine(responseChan, 100)
	}

	if err != nil {
		return nil, nil, nil, err
	}

	if processor == nil {
		return shards, nil, nil, nil
	}

	go func() {
		for {
			response := <-responseChan

			if *response.Type == endStreamResponse || *response.Type == accessDeniedResponse {
				writer.Close()
				seriesClosed <- true
				return
			}
			if !(*response.Type == queryResponse && querySpec.IsExplainQuery()) {
				log.Info("recieved %d points on the response channel", len(response.Series.Points))
				if response.Series != nil && len(response.Series.Points) > 0 {
					writer.Write(response.Series)
				}
			}
		}
	}()

	return shards, processor, seriesClosed, nil
}

func (self *CoordinatorImpl) readFromResposneChannels(processor cluster.QueryProcessor,
	writer SeriesWriter,
	isExplainQuery bool,
	errors chan<- error,
	channels <-chan (<-chan *protocol.Response)) {

	defer close(errors)

	for responseChan := range channels {
		for response := range responseChan {

			//log.Debug("GOT RESPONSE: ", response.Type, response.Series)
			log.Debug("GOT RESPONSE: ", response.Type)
			if *response.Type == endStreamResponse || *response.Type == accessDeniedResponse {
				if response.ErrorMessage == nil {
					break
				}

				err := common.NewQueryError(common.InvalidArgument, *response.ErrorMessage)
				log.Error("Error while executing query: %s", err)
				errors <- err
				return
			}

			if response.Series == nil || len(response.Series.Points) == 0 {
				log.Debug("Series has no points, continue")
				continue
			}

			// if we don't have a processor, yield the point to the writer
			// this happens if shard took care of the query
			// otherwise client will get points from passthrough engine
			if processor != nil {
				// if the data wasn't aggregated at the shard level, aggregate
				// the data here
				log.Debug("YIELDING: %d points with %d columns", len(response.Series.Points), len(response.Series.Fields))
				processor.YieldSeries(response.Series)
				continue
			}

			// If we have EXPLAIN query, we don't write actual points (of
			// response.Type Query) to the client
			if !(*response.Type == queryResponse && isExplainQuery) {
				writer.Write(response.Series)
			}
		}

		// once we're done with a response channel signal queryShards to
		// start querying a new shard
		errors <- nil
	}
	return
}

func (self *CoordinatorImpl) queryShards(querySpec *parser.QuerySpec, shards []*cluster.ShardData,
	errors <-chan error,
	responseChannels chan<- (<-chan *protocol.Response)) error {
	defer close(responseChannels)

	for i := 0; i < len(shards); i++ {
		// readFromResposneChannels will insert an error if an error
		// occured while reading the response. This should immediately
		// stop reading from shards
		err := <-errors
		if err != nil {
			return err
		}
		shard := shards[i]
		bufferSize := shard.QueryResponseBufferSize(querySpec, self.config.LevelDbPointBatchSize)
		if bufferSize > self.config.ClusterMaxResponseBufferSize {
			bufferSize = self.config.ClusterMaxResponseBufferSize
		}
		responseChan := make(chan *protocol.Response, bufferSize)
		// We query shards for data and stream them to query processor
		log.Debug("QUERYING: shard: ", i, shard.String())
		go shard.Query(querySpec, responseChan)
		responseChannels <- responseChan
	}

	return nil
}

func (self *CoordinatorImpl) runQuerySpec(querySpec *parser.QuerySpec, seriesWriter SeriesWriter) error {
	shards, processor, seriesClosed, err := self.getShardsAndProcessor(querySpec, seriesWriter)
	if err != nil {
		return err
	}

	defer func() {
		if processor != nil {
			processor.Close()
			<-seriesClosed
		} else {
			seriesWriter.Close()
		}
	}()

	shardConcurrentLimit := self.config.ConcurrentShardQueryLimit
	if self.shouldQuerySequentially(shards, querySpec) {
		log.Debug("Querying shards sequentially")
		shardConcurrentLimit = 1
	}
	log.Debug("Shard concurrent limit: ", shardConcurrentLimit)

	errors := make(chan error, shardConcurrentLimit)
	for i := 0; i < shardConcurrentLimit; i++ {
		errors <- nil
	}
	responseChannels := make(chan (<-chan *protocol.Response), shardConcurrentLimit)

	go self.readFromResposneChannels(processor, seriesWriter, querySpec.IsExplainQuery(), errors, responseChannels)

	err = self.queryShards(querySpec, shards, errors, responseChannels)

	// make sure we read the rest of the errors and responses
	for _err := range errors {
		if err == nil {
			err = _err
		}
	}

	for responsechan := range responseChannels {
		for response := range responsechan {
			if response.GetType() != endStreamResponse {
				continue
			}
			if response.ErrorMessage != nil && err == nil {
				err = common.NewQueryError(common.InvalidArgument, *response.ErrorMessage)
			}
			break
		}
	}
	return err
}

func (self *CoordinatorImpl) ForceCompaction(user common.User) error {
	if !user.IsClusterAdmin() {
		return fmt.Errorf("Insufficient permissions to force a log compaction")
	}

	return self.raftServer.ForceLogCompaction()
}

func (self *CoordinatorImpl) WriteSeriesData(user common.User, db string, series []*protocol.Series) error {
	if !user.HasWriteAccess(db) {
		return common.NewAuthorizationError("Insufficient permissions to write to %s", db)
	}

	err := self.CommitSeriesData(db, series)
	if err != nil {
		return err
	}

	for _, s := range series {
		self.ProcessContinuousQueries(db, s)
	}

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

			for _, table := range fromClause.Names {
				tableValue := table.Name
				if regex, ok := tableValue.GetCompiledRegex(); ok {
					if regex.MatchString(incomingSeriesName) {
						self.InterpolateValuesAndCommit(query.GetQueryString(), db, series, targetName, false)
					}
				} else {
					if tableValue.Name == incomingSeriesName {
						self.InterpolateValuesAndCommit(query.GetQueryString(), db, series, targetName, false)
					}
				}
			}
		}
	}
}

func (self *CoordinatorImpl) InterpolateValuesAndCommit(query string, db string, series *protocol.Series, targetName string, assignSequenceNumbers bool) error {
	defer common.RecoverFunc(db, query, nil)

	targetName = strings.Replace(targetName, ":series_name", *series.Name, -1)
	type sequenceKey struct {
		seriesName string
		timestamp  int64
	}
	sequenceMap := make(map[sequenceKey]int)
	r, _ := regexp.Compile(`\[.*?\]`)

	if r.MatchString(targetName) {
		serieses := map[string]*protocol.Series{}
		for _, point := range series.Points {
			targetNameWithValues := r.ReplaceAllStringFunc(targetName, func(match string) string {
				fieldName := match[1 : len(match)-1]
				fieldIndex := series.GetFieldIndex(fieldName)
				return point.GetFieldValueAsString(fieldIndex)
			})

			if assignSequenceNumbers {
				key := sequenceKey{targetNameWithValues, *point.Timestamp}
				sequenceMap[key] += 1
				sequenceNumber := uint64(sequenceMap[key])
				point.SequenceNumber = &sequenceNumber
			}

			newSeries := serieses[targetNameWithValues]
			if newSeries == nil {
				newSeries = &protocol.Series{Name: &targetNameWithValues, Fields: series.Fields, Points: []*protocol.Point{point}}
				serieses[targetNameWithValues] = newSeries
				continue
			}
			newSeries.Points = append(newSeries.Points, point)
		}
		seriesSlice := make([]*protocol.Series, 0, len(serieses))
		for _, s := range serieses {
			seriesSlice = append(seriesSlice, s)
		}
		if e := self.CommitSeriesData(db, seriesSlice); e != nil {
			log.Error("Couldn't write data for continuous query: ", e)
		}
	} else {
		newSeries := &protocol.Series{Name: &targetName, Fields: series.Fields, Points: series.Points}

		if assignSequenceNumbers {
			for _, point := range newSeries.Points {
				sequenceMap[sequenceKey{targetName, *point.Timestamp}] += 1
				sequenceNumber := uint64(sequenceMap[sequenceKey{targetName, *point.Timestamp}])
				point.SequenceNumber = &sequenceNumber
			}
		}

		if e := self.CommitSeriesData(db, []*protocol.Series{newSeries}); e != nil {
			log.Error("Couldn't write data for continuous query: ", e)
		}
	}

	return nil
}

func (self *CoordinatorImpl) CommitSeriesData(db string, serieses []*protocol.Series) error {
	now := common.CurrentTime()

	shardToSerieses := map[uint32]map[string]*protocol.Series{}
	shardIdToShard := map[uint32]*cluster.ShardData{}

	for _, series := range serieses {
		if len(series.Points) == 0 {
			return fmt.Errorf("Can't write series with zero points.")
		}

		for _, point := range series.Points {
			if point.Timestamp == nil {
				point.Timestamp = &now
			}
		}

		// sort the points by timestamp
		// TODO: this isn't needed anymore
		series.SortPointsTimeDescending()

		for i := 0; i < len(series.Points); {
			shard, err := self.clusterConfiguration.GetShardToWriteToBySeriesAndTime(db, series.GetName(), series.Points[i].GetTimestamp())
			if err != nil {
				return err
			}
			firstIndex := i
			timestamp := series.Points[i].GetTimestamp()
			for ; i < len(series.Points) && series.Points[i].GetTimestamp() == timestamp; i++ {
				// add all points with the same timestamp
			}
			newSeries := &protocol.Series{Name: series.Name, Fields: series.Fields, Points: series.Points[firstIndex:i:i]}

			shardIdToShard[shard.Id()] = shard
			shardSerieses := shardToSerieses[shard.Id()]
			if shardSerieses == nil {
				shardSerieses = map[string]*protocol.Series{}
				shardToSerieses[shard.Id()] = shardSerieses
			}
			seriesName := series.GetName()
			s := shardSerieses[seriesName]
			if s == nil {
				shardSerieses[seriesName] = newSeries
				continue
			}
			s.Points = append(s.Points, newSeries.Points...)
		}
	}

	log.Info("Writing points to %d shards", len(shardToSerieses))

	for id, serieses := range shardToSerieses {
		shard := shardIdToShard[id]

		seriesesSlice := make([]*protocol.Series, 0, len(serieses))
		for _, s := range serieses {
			seriesesSlice = append(seriesesSlice, s)
		}

		err := self.write(db, seriesesSlice, shard)
		if err != nil {
			log.Error("COORD error writing: ", err)
			return err
		}
	}

	return nil
}

func (self *CoordinatorImpl) write(db string, series []*protocol.Series, shard cluster.Shard) error {
	request := &protocol.Request{Type: &write, Database: &db, MultiSeries: series}
	return shard.Write(request)
}

func (self *CoordinatorImpl) CreateContinuousQuery(user common.User, db string, query string) error {
	if !user.IsClusterAdmin() && !user.IsDbAdmin(db) {
		return common.NewAuthorizationError("Insufficient permissions to create continuous query")
	}

	err := self.raftServer.CreateContinuousQuery(db, query)
	if err != nil {
		return err
	}
	return nil
}

func (self *CoordinatorImpl) DeleteContinuousQuery(user common.User, db string, id uint32) error {
	if !user.IsClusterAdmin() && !user.IsDbAdmin(db) {
		return common.NewAuthorizationError("Insufficient permissions to delete continuous query")
	}

	err := self.raftServer.DeleteContinuousQuery(db, id)
	if err != nil {
		return err
	}
	return nil
}

func (self *CoordinatorImpl) ListContinuousQueries(user common.User, db string) ([]*protocol.Series, error) {
	if !user.IsClusterAdmin() && !user.IsDbAdmin(db) {
		return nil, common.NewAuthorizationError("Insufficient permissions to list continuous queries")
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
		return common.NewAuthorizationError("Insufficient permissions to create database")
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
		return nil, common.NewAuthorizationError("Insufficient permissions to list databases")
	}

	dbs := self.clusterConfiguration.GetDatabases()
	return dbs, nil
}

func (self *CoordinatorImpl) DropDatabase(user common.User, db string) error {
	if !user.IsClusterAdmin() {
		return common.NewAuthorizationError("Insufficient permissions to drop database")
	}

	if err := self.clusterConfiguration.CreateCheckpoint(); err != nil {
		return err
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
		log.Debug("(raft:%s) User %s authenticated succesfully", self.raftServer.(*RaftServer).raftServer.Name(), username)
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

func (self *CoordinatorImpl) CreateClusterAdminUser(requester common.User, username, password string) error {
	if !requester.IsClusterAdmin() {
		return common.NewAuthorizationError("Insufficient permissions")
	}

	if !isValidName(username) {
		return fmt.Errorf("%s isn't a valid username", username)
	}

	hash, err := cluster.HashPassword(password)
	if err != nil {
		return err
	}

	if self.clusterConfiguration.GetClusterAdmin(username) != nil {
		return fmt.Errorf("User %s already exists", username)
	}

	return self.raftServer.SaveClusterAdminUser(&cluster.ClusterAdmin{cluster.CommonUser{Name: username, CacheKey: username, Hash: string(hash)}})
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

func (self *CoordinatorImpl) CreateDbUser(requester common.User, db, username, password string, permissions ...string) error {
	if !requester.IsClusterAdmin() && !requester.IsDbAdmin(db) {
		return common.NewAuthorizationError("Insufficient permissions")
	}

	if username == "" {
		return fmt.Errorf("Username cannot be empty")
	}

	if !isValidName(username) {
		return fmt.Errorf("%s isn't a valid username", username)
	}

	hash, err := cluster.HashPassword(password)
	if err != nil {
		return err
	}

	self.CreateDatabase(requester, db, uint8(1)) // ignore the error since the db may exist
	if self.clusterConfiguration.GetDbUser(db, username) != nil {
		return fmt.Errorf("User %s already exists", username)
	}
	readMatcher := []*cluster.Matcher{&cluster.Matcher{true, ".*"}}
	writeMatcher := []*cluster.Matcher{&cluster.Matcher{true, ".*"}}
	switch len(permissions) {
	case 0:
	case 2:
		readMatcher[0].Name = permissions[0]
		writeMatcher[0].Name = permissions[0]
	}
	log.Debug("(raft:%s) Creating user %s:%s", self.raftServer.(*RaftServer).raftServer.Name(), db, username)
	return self.raftServer.SaveDbUser(&cluster.DbUser{cluster.CommonUser{
		Name:     username,
		Hash:     string(hash),
		CacheKey: db + "%" + username,
	}, db, readMatcher, writeMatcher, false})
}

func (self *CoordinatorImpl) DeleteDbUser(requester common.User, db, username string) error {
	if !requester.IsClusterAdmin() && !requester.IsDbAdmin(db) {
		return common.NewAuthorizationError("Insufficient permissions")
	}

	user := self.clusterConfiguration.GetDbUser(db, username)
	if user == nil {
		return fmt.Errorf("User %s doesn't exist", username)
	}
	user.CommonUser.IsUserDeleted = true
	return self.raftServer.SaveDbUser(user)
}

func (self *CoordinatorImpl) ListDbUsers(requester common.User, db string) ([]common.User, error) {
	if !requester.IsClusterAdmin() && !requester.IsDbAdmin(db) {
		return nil, common.NewAuthorizationError("Insufficient permissions")
	}

	return self.clusterConfiguration.GetDbUsers(db), nil
}

func (self *CoordinatorImpl) GetDbUser(requester common.User, db string, username string) (common.User, error) {
	if !requester.IsClusterAdmin() && !requester.IsDbAdmin(db) {
		return nil, common.NewAuthorizationError("Insufficient permissions")
	}

	dbUser := self.clusterConfiguration.GetDbUser(db, username)
	if dbUser == nil {
		return nil, fmt.Errorf("Invalid username %s", username)
	}

	return dbUser, nil
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

func (self *CoordinatorImpl) ChangeDbUserPermissions(requester common.User, db, username, readPermissions, writePermissions string) error {
	if !requester.IsClusterAdmin() && !requester.IsDbAdmin(db) {
		return common.NewAuthorizationError("Insufficient permissions")
	}

	return self.raftServer.ChangeDbUserPermissions(db, username, readPermissions, writePermissions)
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
	return !strings.Contains(name, "%")
}
