package coordinator

import (
	"fmt"
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

type Coordinator struct {
	clusterConfiguration *cluster.ClusterConfiguration
	raftServer           *RaftServer
	config               *configuration.Configuration
	permissions          Permissions
}

func NewCoordinator(
	config *configuration.Configuration,
	raftServer *RaftServer,
	clusterConfiguration *cluster.ClusterConfiguration) *Coordinator {
	coordinator := &Coordinator{
		config:               config,
		clusterConfiguration: clusterConfiguration,
		raftServer:           raftServer,
		permissions:          Permissions{},
	}

	return coordinator
}

func (self *Coordinator) RunQuery(user common.User, database string, queryString string, p engine.Processor) (err error) {
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
		err := self.runSingleQuery(user, database, query, p)
		if err != nil {
			return err
		}
	}
	return nil
}

func (self *Coordinator) runSingleQuery(user common.User, db string, q *parser.Query, p engine.Processor) error {
	querySpec := parser.NewQuerySpec(user, db, q)

	if ok, err := self.permissions.CheckQueryPermissions(user, db, querySpec); !ok {
		return err
	}

	switch qt := q.Type(); qt {
	// administrative
	case parser.DropContinuousQuery:
		return self.runDropContinuousQuery(user, db, uint32(q.DropQuery.Id))
	case parser.ListContinuousQueries:
		return self.runListContinuousQueries(user, db, p)
	case parser.Continuous:
		return self.runContinuousQuery(user, db, q.GetQueryString())
	case parser.ListSeries:
		return self.runListSeriesQuery(querySpec, p)
		// Data queries
	case parser.Delete:
		return self.runDeleteQuery(querySpec, p)
	case parser.DropSeries:
		return self.runDropSeriesQuery(querySpec)
	case parser.Select:
		return self.runQuerySpec(querySpec, p)
	default:
		return fmt.Errorf("Can't handle query %s", qt)
	}
}

func (self *Coordinator) runListContinuousQueries(user common.User, db string, p engine.Processor) error {
	queries, err := self.ListContinuousQueries(user, db)
	if err != nil {
		return err
	}
	for _, q := range queries {
		if ok, err := p.Yield(q); !ok || err != nil {
			return err
		}
	}
	return nil
}

func (self *Coordinator) runListSeriesQuery(querySpec *parser.QuerySpec, p engine.Processor) error {
	allSeries := self.clusterConfiguration.MetaStore.GetSeriesForDatabase(querySpec.Database())
	matchingSeries := allSeries
	q := querySpec.Query().GetListSeriesQuery()
	if q.HasRegex() {
		matchingSeries = nil
		regex := q.GetRegex()
		for _, s := range allSeries {
			if !regex.MatchString(s) {
				continue
			}
			matchingSeries = append(matchingSeries, s)
		}
	}
	name := "list_series_result"
	var fields []string
	points := make([]*protocol.Point, len(matchingSeries))

	if q.IncludeSpaces {
		fields = []string{"name", "space"}
		spaces := self.clusterConfiguration.GetShardSpacesForDatabase(querySpec.Database())

		for i, s := range matchingSeries {
			spaceName := ""
			for _, sp := range spaces {
				if sp.MatchesSeries(s) {
					spaceName = sp.Name
					break
				}
			}
			fieldValues := []*protocol.FieldValue{
				{StringValue: proto.String(s)},
				{StringValue: proto.String(spaceName)},
			}
			points[i] = &protocol.Point{Values: fieldValues}
		}
	} else {
		fields = []string{"name"}
		for i, s := range matchingSeries {
			fieldValues := []*protocol.FieldValue{
				{StringValue: proto.String(s)},
			}
			points[i] = &protocol.Point{Values: fieldValues}
		}
	}

	seriesResult := &protocol.Series{Name: &name, Fields: fields, Points: points}
	_, err := p.Yield(seriesResult)
	return err
}

func (self *Coordinator) runDeleteQuery(querySpec *parser.QuerySpec, p engine.Processor) error {
	if err := self.clusterConfiguration.CreateCheckpoint(); err != nil {
		return err
	}
	querySpec.RunAgainstAllServersInShard = true
	return self.runQuerySpec(querySpec, p)
}

func (self *Coordinator) runDropSeriesQuery(querySpec *parser.QuerySpec) error {
	user := querySpec.User()
	db := querySpec.Database()
	series := querySpec.Query().DropSeriesQuery.GetTableName()
	if ok, err := self.permissions.AuthorizeDropSeries(user, db, series); !ok {
		return err
	}
	err := self.raftServer.DropSeries(db, series)
	if err != nil {
		return err
	}
	return nil
}

func (self *Coordinator) shouldQuerySequentially(shards cluster.Shards, querySpec *parser.QuerySpec) bool {
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

	if !shards.ShouldAggregateLocally(querySpec) {
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

func (self *Coordinator) getShardsAndProcessor(querySpec *parser.QuerySpec, writer engine.Processor) ([]*cluster.ShardData, engine.Processor, error) {
	shards, err := self.clusterConfiguration.GetShardsForQuery(querySpec)
	if err != nil {
		return nil, nil, err
	}
	shouldAggregateLocally := shards.ShouldAggregateLocally(querySpec)

	q := querySpec.SelectQuery()
	if q == nil {
		return shards, writer, nil
	}

	if !shouldAggregateLocally {
		// if we should aggregate in the coordinator (i.e. aggregation
		// isn't happening locally at the shard level), create an engine
		writer, err = engine.NewQueryEngine(writer, q)
		return shards, writer, err
	}

	// if we have a query with limit, then create an engine, or we can
	// make the passthrough limit aware
	writer = engine.NewPassthroughEngineWithLimit(writer, 100, q.Limit)
	return shards, writer, nil
}

func (self *Coordinator) queryShards(querySpec *parser.QuerySpec, shards []*cluster.ShardData, p *MergeChannelProcessor) error {
	for i, s := range shards {
		// readFromResponseChannels will insert an error if an error
		// occured while reading the response. This should immediately
		// stop reading from shards
		bufferSize := s.QueryResponseBufferSize(querySpec, self.config.StoragePointBatchSize)
		if bufferSize > self.config.ClusterMaxResponseBufferSize {
			bufferSize = self.config.ClusterMaxResponseBufferSize
		}
		c, err := p.NextChannel(bufferSize)
		if err != nil {
			return err
		}
		// We query shards for data and stream them to query processor
		log.Debug("QUERYING: shard: %d %v", i, s.String())
		go s.Query(querySpec, c)
	}

	return nil
}

// We call this function only if we have a Select query (not continuous) or Delete query
func (self *Coordinator) runQuerySpec(querySpec *parser.QuerySpec, p engine.Processor) error {
	shards, processor, err := self.getShardsAndProcessor(querySpec, p)
	if err != nil {
		return err
	}

	if len(shards) == 0 {
		return processor.Close()
	}

	shardConcurrentLimit := self.config.ConcurrentShardQueryLimit
	if self.shouldQuerySequentially(shards, querySpec) {
		log.Debug("Querying shards sequentially")
		shardConcurrentLimit = 1
	}
	log.Debug("Shard concurrent limit: %d", shardConcurrentLimit)

	mcp := NewMergeChannelProcessor(processor, shardConcurrentLimit)

	go mcp.ProcessChannels()

	if err := self.queryShards(querySpec, shards, mcp); err != nil {
		log.Error("Error while querying shards: %s", err)
		mcp.Close()
		return err
	}

	if err := mcp.Close(); err != nil {
		log.Error("Error while querying shards: %s", err)
		return err
	}

	return processor.Close()
}

func (self *Coordinator) ProcessContinuousQueries(db string, series *protocol.Series) {
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

func (self *Coordinator) InterpolateValuesAndCommit(query string, db string, series *protocol.Series, targetName string, assignSequenceNumbers bool) error {
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

func (self *Coordinator) CommitSeriesData(db string, serieses []*protocol.Series, sync bool) error {
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
			shardSerieses[seriesName] = s.Merge(newSeries)
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

func (self *Coordinator) write(db string, series []*protocol.Series, shard cluster.Shard, sync bool) error {
	// replace all the field names, or error out if we can't assign the field ids.
	err := self.clusterConfiguration.MetaStore.ReplaceFieldNamesWithFieldIds(db, series)
	if err != nil {
		return err
	}

	return self.writeWithoutAssigningId(db, series, shard, sync)
}

func (self *Coordinator) writeWithoutAssigningId(db string, series []*protocol.Series, shard cluster.Shard, sync bool) error {
	request := &protocol.Request{
		Type:        protocol.Request_WRITE.Enum(),
		Database:    &db,
		MultiSeries: series,
	}
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

func (self *Coordinator) runContinuousQuery(user common.User, db string, query string) error {
	if ok, err := self.permissions.AuthorizeCreateContinuousQuery(user, db); !ok {
		return err
	}

	err := self.raftServer.CreateContinuousQuery(db, query)
	if err != nil {
		return err
	}
	return nil
}

func (self *Coordinator) runDropContinuousQuery(user common.User, db string, id uint32) error {
	if ok, err := self.permissions.AuthorizeDeleteContinuousQuery(user, db); !ok {
		return err
	}

	err := self.raftServer.DeleteContinuousQuery(db, id)
	if err != nil {
		return err
	}
	return nil
}
