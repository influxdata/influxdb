package coordinator

import (
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"

	"code.google.com/p/goprotobuf/proto"
	log "code.google.com/p/log4go"
	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/common"
	"github.com/influxdb/influxdb/configuration"
	"github.com/influxdb/influxdb/engine"
	"github.com/influxdb/influxdb/parser"
	"github.com/influxdb/influxdb/protocol"
)

type CoordinatorImpl struct {
	clusterConfiguration *cluster.ClusterConfiguration
	raftServer           ClusterConsensus
	config               *configuration.Configuration
	metastore            Metastore
	permissions          Permissions
}

type Metastore interface {
	ReplaceFieldNamesWithFieldIds(database string, series []*protocol.Series) error
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

func NewCoordinatorImpl(
	config *configuration.Configuration,
	raftServer ClusterConsensus,
	clusterConfiguration *cluster.ClusterConfiguration,
	metastore Metastore) *CoordinatorImpl {
	coordinator := &CoordinatorImpl{
		config:               config,
		clusterConfiguration: clusterConfiguration,
		raftServer:           raftServer,
		metastore:            metastore,
		permissions:          Permissions{},
	}

	return coordinator
}

func (self *CoordinatorImpl) RunQuery(user common.User, database string, queryString string, seriesWriter SeriesWriter) (err error) {
	log.Info("Start Query: db: %s, u: %s, q: %s", database, user.GetName(), queryString)
	defer func(t time.Time) {
		log.Debug("End Query: db: %s, u: %s, q: %s, t: %s", database, user.GetName(), queryString, time.Now().Sub(t))
	}(time.Now())
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
	series := self.clusterConfiguration.MetaStore.GetSeriesForDatabase(querySpec.Database())
	name := "list_series_result"
	fields := []string{"name"}
	points := make([]*protocol.Point, len(series), len(series))

	for i, s := range series {
		fieldValues := []*protocol.FieldValue{{StringValue: proto.String(s)}}
		points[i] = &protocol.Point{Values: fieldValues}
	}

	seriesResult := &protocol.Series{Name: &name, Fields: fields, Points: points}
	seriesWriter.Write(seriesResult)
	seriesWriter.Close()
	return nil
}

func (self *CoordinatorImpl) runDeleteQuery(querySpec *parser.QuerySpec, seriesWriter SeriesWriter) error {
	user := querySpec.User()
	db := querySpec.Database()
	if ok, err := self.permissions.AuthorizeDeleteQuery(user, db); !ok {
		return err
	}
	querySpec.RunAgainstAllServersInShard = true
	return self.runQuerySpec(querySpec, seriesWriter)
}

func (self *CoordinatorImpl) runDropSeriesQuery(querySpec *parser.QuerySpec, seriesWriter SeriesWriter) error {
	user := querySpec.User()
	db := querySpec.Database()
	series := querySpec.Query().DropSeriesQuery.GetTableName()
	if ok, err := self.permissions.AuthorizeDropSeries(user, db, series); !ok {
		return err
	}
	defer seriesWriter.Close()
	fmt.Println("DROP series")
	err := self.raftServer.DropSeries(db, series)
	if err != nil {
		return err
	}
	fmt.Println("DROP returning nil")
	return nil
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
		bufferSize := shard.QueryResponseBufferSize(querySpec, self.config.StoragePointBatchSize)
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
	shards := self.clusterConfiguration.GetShardsForQuery(querySpec)
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
				if response.Series != nil && len(response.Series.Points) > 0 {
					writer.Write(response.Series)
				}
			}
		}
	}()

	return shards, processor, seriesClosed, nil
}

func (self *CoordinatorImpl) readFromResponseChannels(processor cluster.QueryProcessor,
	writer SeriesWriter,
	isExplainQuery bool,
	errors chan<- error,
	responseChannels <-chan (<-chan *protocol.Response)) {

	defer close(errors)

	for responseChan := range responseChannels {
		for response := range responseChan {

			//log.Debug("GOT RESPONSE: ", response.Type, response.Series)
			log.Debug("GOT RESPONSE: %v", response.Type)
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
				log.Debug("YIELDING: %d points with %d columns for %s", len(response.Series.Points), len(response.Series.Fields), response.Series.GetName())
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
		// readFromResponseChannels will insert an error if an error
		// occured while reading the response. This should immediately
		// stop reading from shards
		err := <-errors
		if err != nil {
			return err
		}
		shard := shards[i]
		bufferSize := shard.QueryResponseBufferSize(querySpec, self.config.StoragePointBatchSize)
		if bufferSize > self.config.ClusterMaxResponseBufferSize {
			bufferSize = self.config.ClusterMaxResponseBufferSize
		}
		responseChan := make(chan *protocol.Response, bufferSize)
		// We query shards for data and stream them to query processor
		log.Debug("QUERYING: shard: %d %v", i, shard.String())
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

	if len(shards) == 0 {
		return fmt.Errorf("Couldn't look up columns")
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
	log.Debug("Shard concurrent limit: %d", shardConcurrentLimit)

	errors := make(chan error, shardConcurrentLimit)
	for i := 0; i < shardConcurrentLimit; i++ {
		errors <- nil
	}
	responseChannels := make(chan (<-chan *protocol.Response), shardConcurrentLimit)

	go self.readFromResponseChannels(processor, seriesWriter, querySpec.IsExplainQuery(), errors, responseChannels)

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
	// make sure that the db exist
	if !self.clusterConfiguration.DatabasesExists(db) {
		return fmt.Errorf("Database %s doesn't exist", db)
	}

	for _, s := range series {
		seriesName := s.GetName()
		if user.HasWriteAccess(seriesName) {
			continue
		}
		return common.NewAuthorizationError("User %s doesn't have write permissions for %s", user.GetName(), seriesName)
	}

	err := self.CommitSeriesData(db, series, false)
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

	// get the fields that are used in the target name
	fieldsInTargetName := r.FindAllString(targetName, -1)
	fieldsIndeces := make([]int, 0, len(fieldsInTargetName))
	for i, f := range fieldsInTargetName {
		f = f[1 : len(f)-1]
		fieldsIndeces = append(fieldsIndeces, series.GetFieldIndex(f))
		fieldsInTargetName[i] = f
	}

	fields := make([]string, 0, len(series.Fields)-len(fieldsIndeces))

	// remove the fields used in the target name from the series fields
nextfield:
	for i, f := range series.Fields {
		for _, fi := range fieldsIndeces {
			if fi == i {
				continue nextfield
			}
		}
		fields = append(fields, f)
	}

	if r.MatchString(targetName) {
		serieses := map[string]*protocol.Series{}
		for _, point := range series.Points {
			fieldIndex := 0
			targetNameWithValues := r.ReplaceAllStringFunc(targetName, func(_ string) string {
				value := point.GetFieldValueAsString(fieldsIndeces[fieldIndex])
				fieldIndex++
				return value
			})

			p := &protocol.Point{
				Values:         make([]*protocol.FieldValue, 0, len(point.Values)-len(fieldsIndeces)),
				Timestamp:      point.Timestamp,
				SequenceNumber: point.SequenceNumber,
			}

			// remove the fields used in the target name from the series fields
		nextvalue:
			for i, v := range point.Values {
				for _, fi := range fieldsIndeces {
					if fi == i {
						continue nextvalue
					}
				}
				p.Values = append(p.Values, v)
			}

			if assignSequenceNumbers {
				key := sequenceKey{targetNameWithValues, *p.Timestamp}
				sequenceMap[key] += 1
				sequenceNumber := uint64(sequenceMap[key])
				p.SequenceNumber = &sequenceNumber
			}

			newSeries := serieses[targetNameWithValues]
			if newSeries == nil {
				newSeries = &protocol.Series{Name: &targetNameWithValues, Fields: fields, Points: []*protocol.Point{p}}
				serieses[targetNameWithValues] = newSeries
				continue
			}
			newSeries.Points = append(newSeries.Points, p)
		}
		seriesSlice := make([]*protocol.Series, 0, len(serieses))
		for _, s := range serieses {
			seriesSlice = append(seriesSlice, s)
		}
		if e := self.CommitSeriesData(db, seriesSlice, true); e != nil {
			log.Error("Couldn't write data for continuous query: ", e)
		}
	} else {
		newSeries := &protocol.Series{Name: &targetName, Fields: fields, Points: series.Points}

		if assignSequenceNumbers {
			for _, point := range newSeries.Points {
				sequenceMap[sequenceKey{targetName, *point.Timestamp}] += 1
				sequenceNumber := uint64(sequenceMap[sequenceKey{targetName, *point.Timestamp}])
				point.SequenceNumber = &sequenceNumber
			}
		}

		if e := self.CommitSeriesData(db, []*protocol.Series{newSeries}, true); e != nil {
			log.Error("Couldn't write data for continuous query: ", e)
		}
	}

	return nil
}

func (self *CoordinatorImpl) CommitSeriesData(db string, serieses []*protocol.Series, sync bool) error {
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
			if len(series.GetName()) == 0 {
				return fmt.Errorf("Series name cannot be empty")
			}

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
			shardSerieses[seriesName] = common.MergeSeries(s, newSeries)
		}
	}

	for id, serieses := range shardToSerieses {
		shard := shardIdToShard[id]

		seriesesSlice := make([]*protocol.Series, 0, len(serieses))
		for _, s := range serieses {
			seriesesSlice = append(seriesesSlice, s)
		}

		err := self.write(db, seriesesSlice, shard, sync)
		if err != nil {
			log.Error("COORD error writing: ", err)
			return err
		}
	}

	return nil
}

func (self *CoordinatorImpl) write(db string, series []*protocol.Series, shard cluster.Shard, sync bool) error {
	// replace all the field names, or error out if we can't assign the field ids.
	err := self.metastore.ReplaceFieldNamesWithFieldIds(db, series)
	if err != nil {
		return err
	}

	return self.writeWithoutAssigningId(db, series, shard, sync)
}

func (self *CoordinatorImpl) writeWithoutAssigningId(db string, series []*protocol.Series, shard cluster.Shard, sync bool) error {
	request := &protocol.Request{Type: &write, Database: &db, MultiSeries: series}
	// break the request if it's too big
	if request.Size() >= MAX_REQUEST_SIZE {
		if l := len(series); l > 1 {
			// create two requests with half the serie
			if err := self.writeWithoutAssigningId(db, series[:l/2], shard, sync); err != nil {
				return err
			}
			return self.writeWithoutAssigningId(db, series[l/2:], shard, sync)
		}

		// otherwise, split the points of the only series
		s := series[0]
		l := len(s.Points)
		s1 := &protocol.Series{Name: s.Name, FieldIds: s.FieldIds, Points: s.Points[:l/2]}
		if err := self.writeWithoutAssigningId(db, []*protocol.Series{s1}, shard, sync); err != nil {
			return err
		}
		s2 := &protocol.Series{Name: s.Name, FieldIds: s.FieldIds, Points: s.Points[l/2:]}
		return self.writeWithoutAssigningId(db, []*protocol.Series{s2}, shard, sync)
	}

	// if we received a synchronous write, then this is coming from the
	// continuous queries which have the sequence numbers assigned
	if sync {
		return shard.SyncWrite(request, false)
	}

	// If the shard isn't replicated do a syncrhonous write
	if shard.ReplicationFactor() <= 1 {
		// assign sequenceNumber and write synchronously
		return shard.SyncWrite(request, true)
	}
	return shard.Write(request)
}

func (self *CoordinatorImpl) CreateContinuousQuery(user common.User, db string, query string) error {
	if ok, err := self.permissions.AuthorizeCreateContinuousQuery(user, db); !ok {
		return err
	}

	err := self.raftServer.CreateContinuousQuery(db, query)
	if err != nil {
		return err
	}
	return nil
}

func (self *CoordinatorImpl) DeleteContinuousQuery(user common.User, db string, id uint32) error {
	if ok, err := self.permissions.AuthorizeDeleteContinuousQuery(user, db); !ok {
		return err
	}

	err := self.raftServer.DeleteContinuousQuery(db, id)
	if err != nil {
		return err
	}
	return nil
}

func (self *CoordinatorImpl) ListContinuousQueries(user common.User, db string) ([]*protocol.Series, error) {
	if ok, err := self.permissions.AuthorizeListContinuousQueries(user, db); !ok {
		return nil, err
	}

	queries := self.clusterConfiguration.GetContinuousQueries(db)
	points := []*protocol.Point{}

	for _, query := range queries {
		queryId := int64(query.Id)
		queryString := query.Query
		points = append(points, &protocol.Point{
			Values: []*protocol.FieldValue{
				{Int64Value: &queryId},
				{StringValue: &queryString},
			},
		})
	}
	seriesName := "continuous queries"
	series := []*protocol.Series{{
		Name:   &seriesName,
		Fields: []string{"id", "query"},
		Points: points,
	}}
	return series, nil
}

func (self *CoordinatorImpl) CreateDatabase(user common.User, db string) error {
	if ok, err := self.permissions.AuthorizeCreateDatabase(user); !ok {
		return err
	}

	if !isValidName(db) {
		return fmt.Errorf("%s isn't a valid db name", db)
	}

	err := self.raftServer.CreateDatabase(db)
	if err != nil {
		return err
	}
	return nil
}

func (self *CoordinatorImpl) ListDatabases(user common.User) ([]*cluster.Database, error) {
	if ok, err := self.permissions.AuthorizeListDatabases(user); !ok {
		return nil, err
	}

	dbs := self.clusterConfiguration.GetDatabases()
	return dbs, nil
}

func (self *CoordinatorImpl) DropDatabase(user common.User, db string) error {
	if ok, err := self.permissions.AuthorizeDropDatabase(user); !ok {
		return err
	}

	if err := self.clusterConfiguration.CreateCheckpoint(); err != nil {
		return err
	}

	return self.raftServer.DropDatabase(db)
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
	if ok, err := self.permissions.AuthorizeListClusterAdmins(requester); !ok {
		return nil, err
	}

	return self.clusterConfiguration.GetClusterAdmins(), nil
}

func (self *CoordinatorImpl) CreateClusterAdminUser(requester common.User, username, password string) error {
	if ok, err := self.permissions.AuthorizeCreateClusterAdmin(requester); !ok {
		return err
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
	if ok, err := self.permissions.AuthorizeDeleteClusterAdmin(requester); !ok {
		return err
	}

	user := self.clusterConfiguration.GetClusterAdmin(username)
	if user == nil {
		return fmt.Errorf("User %s doesn't exists", username)
	}

	user.CommonUser.IsUserDeleted = true
	return self.raftServer.SaveClusterAdminUser(user)
}

func (self *CoordinatorImpl) ChangeClusterAdminPassword(requester common.User, username, password string) error {
	if ok, err := self.permissions.AuthorizeChangeClusterAdminPassword(requester); !ok {
		return err
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
	if ok, err := self.permissions.AuthorizeCreateDbUser(requester, db); !ok {
		return err
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

	if !self.clusterConfiguration.DatabaseExists(db) {
		return fmt.Errorf("No such database %s", db)
	}

	if self.clusterConfiguration.GetDbUser(db, username) != nil {
		return fmt.Errorf("User %s already exists", username)
	}
	readMatcher := []*cluster.Matcher{{true, ".*"}}
	writeMatcher := []*cluster.Matcher{{true, ".*"}}
	switch len(permissions) {
	case 0:
	case 2:
		readMatcher[0].Name = permissions[0]
		writeMatcher[0].Name = permissions[1]
	}
	log.Debug("(raft:%s) Creating user %s:%s", self.raftServer.(*RaftServer).raftServer.Name(), db, username)
	return self.raftServer.SaveDbUser(&cluster.DbUser{cluster.CommonUser{
		Name:     username,
		Hash:     string(hash),
		CacheKey: db + "%" + username,
	}, db, readMatcher, writeMatcher, false})
}

func (self *CoordinatorImpl) DeleteDbUser(requester common.User, db, username string) error {
	if ok, err := self.permissions.AuthorizeDeleteDbUser(requester, db); !ok {
		return err
	}

	user := self.clusterConfiguration.GetDbUser(db, username)
	if user == nil {
		return fmt.Errorf("User %s doesn't exist", username)
	}
	user.CommonUser.IsUserDeleted = true
	return self.raftServer.SaveDbUser(user)
}

func (self *CoordinatorImpl) ListDbUsers(requester common.User, db string) ([]common.User, error) {
	if ok, err := self.permissions.AuthorizeListDbUsers(requester, db); !ok {
		return nil, err
	}

	return self.clusterConfiguration.GetDbUsers(db), nil
}

func (self *CoordinatorImpl) GetDbUser(requester common.User, db string, username string) (common.User, error) {
	if ok, err := self.permissions.AuthorizeGetDbUser(requester, db); !ok {
		return nil, err
	}

	dbUser := self.clusterConfiguration.GetDbUser(db, username)
	if dbUser == nil {
		return nil, fmt.Errorf("Invalid username %s", username)
	}

	return dbUser, nil
}

func (self *CoordinatorImpl) ChangeDbUserPassword(requester common.User, db, username, password string) error {
	if ok, err := self.permissions.AuthorizeChangeDbUserPassword(requester, db, username); !ok {
		return err
	}

	hash, err := cluster.HashPassword(password)
	if err != nil {
		return err
	}
	return self.raftServer.ChangeDbUserPassword(db, username, hash)
}

func (self *CoordinatorImpl) ChangeDbUserPermissions(requester common.User, db, username, readPermissions, writePermissions string) error {
	if ok, err := self.permissions.AuthorizeChangeDbUserPermissions(requester, db); !ok {
		return err
	}

	return self.raftServer.ChangeDbUserPermissions(db, username, readPermissions, writePermissions)
}

func (self *CoordinatorImpl) SetDbAdmin(requester common.User, db, username string, isAdmin bool) error {
	if ok, err := self.permissions.AuthorizeGrantDbUserAdmin(requester, db); !ok {
		return err
	}

	user := self.clusterConfiguration.GetDbUser(db, username)
	if user == nil {
		return fmt.Errorf("Invalid username %s", username)
	}
	user.IsAdmin = isAdmin
	self.raftServer.SaveDbUser(user)
	return nil
}

func (self *CoordinatorImpl) ConnectToProtobufServers(localRaftName string) error {
	log.Info("Connecting to other nodes in the cluster")

	for _, server := range self.clusterConfiguration.Servers() {
		if server.RaftName != localRaftName {
			server.Connect()
		}
	}
	return nil
}

func isValidName(name string) bool {
	return !strings.Contains(name, "%")
}
