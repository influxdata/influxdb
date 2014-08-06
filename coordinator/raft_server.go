package coordinator

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	log "code.google.com/p/log4go"
	"github.com/gorilla/mux"
	"github.com/influxdb/influxdb/_vendor/raft"
	"github.com/influxdb/influxdb/cluster"
	"github.com/influxdb/influxdb/common"
	"github.com/influxdb/influxdb/configuration"
	"github.com/influxdb/influxdb/parser"
	"github.com/influxdb/influxdb/protocol"
)

const (
	DEFAULT_ROOT_PWD        = "root"
	DEFAULT_ROOT_PWD_ENVKEY = "INFLUXDB_INIT_PWD"
	RAFT_NAME_SIZE          = 8
)

// The raftd server is a combination of the Raft server and an HTTP
// server which acts as the transport.
type RaftServer struct {
	name                     string
	path                     string
	bind_address             string
	router                   *mux.Router
	raftServer               raft.Server
	httpServer               *http.Server
	clusterConfig            *cluster.ClusterConfiguration
	mutex                    sync.RWMutex
	listener                 net.Listener
	closing                  bool
	config                   *configuration.Configuration
	notLeader                chan bool
	coordinator              *CoordinatorImpl
	processContinuousQueries bool
}

var registeredCommands bool

// Creates a new server.
func NewRaftServer(config *configuration.Configuration, clusterConfig *cluster.ClusterConfiguration) *RaftServer {
	// raft.SetLogLevel(raft.Debug)
	if !registeredCommands {
		registeredCommands = true
		for _, command := range internalRaftCommands {
			raft.RegisterCommand(command)
		}
	}

	s := &RaftServer{
		path:          config.RaftDir,
		clusterConfig: clusterConfig,
		notLeader:     make(chan bool, 1),
		router:        mux.NewRouter(),
		config:        config,
	}
	// Read existing name or generate a new one.
	if b, err := ioutil.ReadFile(filepath.Join(s.path, "name")); err == nil {
		s.name = string(b)
	} else {
		var i uint64
		if _, err := os.Stat("/dev/urandom"); err == nil {
			log.Info("Using /dev/urandom to initialize the raft server name")
			f, err := os.Open("/dev/urandom")
			if err != nil {
				panic(err)
			}
			defer f.Close()
			readBytes := 0
			b := make([]byte, RAFT_NAME_SIZE)
			for readBytes < RAFT_NAME_SIZE {
				n, err := f.Read(b[readBytes:])
				if err != nil {
					panic(err)
				}
				readBytes += n
			}
			err = binary.Read(bytes.NewBuffer(b), binary.BigEndian, &i)
			if err != nil {
				panic(err)
			}
		} else {
			log.Info("Using rand package to generate raft server name")
			rand.Seed(time.Now().UnixNano())
			i = uint64(rand.Int())
		}
		s.name = fmt.Sprintf("%08x", i)
		log.Info("Setting raft name to %s", s.name)
		if err = ioutil.WriteFile(filepath.Join(s.path, "name"), []byte(s.name), 0644); err != nil {
			panic(err)
		}
	}

	return s
}

func (s *RaftServer) GetRaftName() string {
	return s.name
}

func (s *RaftServer) leaderConnectString() (string, bool) {
	leader := s.raftServer.Leader()
	peers := s.raftServer.Peers()
	if peer, ok := peers[leader]; !ok {
		return "", false
	} else {
		return peer.ConnectionString, true
	}
}

func (s *RaftServer) doOrProxyCommand(command raft.Command) (interface{}, error) {
	var err error
	var value interface{}
	for i := 0; i < 3; i++ {
		value, err = s.doOrProxyCommandOnce(command)
		if err == nil {
			return value, nil
		}
		if strings.Contains(err.Error(), "node failure") {
			continue
		}
		return nil, err
	}
	return nil, err
}

func (s *RaftServer) doOrProxyCommandOnce(command raft.Command) (interface{}, error) {

	if s.raftServer.State() == raft.Leader {
		value, err := s.raftServer.Do(command)
		if err != nil {
			log.Error("Cannot run command %#v. %s", command, err)
		}
		return value, err
	} else {
		if leader, ok := s.leaderConnectString(); !ok {
			return nil, errors.New("Couldn't connect to the cluster leader...")
		} else {
			return SendCommandToServer(leader, command)
		}
	}
}

func SendCommandToServer(url string, command raft.Command) (interface{}, error) {
	var b bytes.Buffer
	if err := json.NewEncoder(&b).Encode(command); err != nil {
		return nil, err
	}
	resp, err := http.Post(url+"/process_command/"+command.CommandName(), "application/json", &b)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err2 := ioutil.ReadAll(resp.Body)

	if resp.StatusCode != 200 {
		return nil, errors.New(strings.TrimSpace(string(body)))
	}

	return body, err2

}

func (s *RaftServer) CreateDatabase(name string) error {
	command := NewCreateDatabaseCommand(name)
	_, err := s.doOrProxyCommand(command)
	return err
}

func (s *RaftServer) DropDatabase(name string) error {
	command := NewDropDatabaseCommand(name)
	_, err := s.doOrProxyCommand(command)
	// TODO: Dropping database from the metastore is synchronous, but the underlying data
	//       delete is asynchronous. If the server crashes or restarts while this is happening
	//       there will be orphaned data sitting around. Not a huge deal, but we should fix this
	//       at some point.
	// force a log compaction because we don't want this replaying after a server restarts
	if err == nil {
		err = s.ForceLogCompaction()
	}
	return err
}

func (s *RaftServer) SaveDbUser(u *cluster.DbUser) error {
	command := NewSaveDbUserCommand(u)
	_, err := s.doOrProxyCommand(command)
	return err
}

func (s *RaftServer) ChangeDbUserPassword(db, username string, hash []byte) error {
	command := NewChangeDbUserPasswordCommand(db, username, string(hash))
	_, err := s.doOrProxyCommand(command)
	return err
}

func (s *RaftServer) ChangeDbUserPermissions(db, username, readPermissions, writePermissions string) error {
	command := NewChangeDbUserPermissionsCommand(db, username, readPermissions, writePermissions)
	_, err := s.doOrProxyCommand(command)
	return err
}

func (s *RaftServer) SaveClusterAdminUser(u *cluster.ClusterAdmin) error {
	command := NewSaveClusterAdminCommand(u)
	_, err := s.doOrProxyCommand(command)
	return err
}

func (s *RaftServer) CreateRootUser() error {
	u := &cluster.ClusterAdmin{cluster.CommonUser{Name: "root", Hash: "", IsUserDeleted: false, CacheKey: "root"}}
	password := os.Getenv(DEFAULT_ROOT_PWD_ENVKEY)
	if password == "" {
		password = DEFAULT_ROOT_PWD
	}
	hash, _ := cluster.HashPassword(password)
	u.ChangePassword(string(hash))
	return s.SaveClusterAdminUser(u)
}

func (s *RaftServer) SetContinuousQueryTimestamp(timestamp time.Time) error {
	command := NewSetContinuousQueryTimestampCommand(timestamp)
	_, err := s.doOrProxyCommand(command)
	return err
}

func (s *RaftServer) CreateContinuousQuery(db string, query string) error {
	selectQuery, err := parser.ParseSelectQuery(query)
	if err != nil {
		return fmt.Errorf("Failed to parse continuous query: %s", query)
	}

	if !selectQuery.IsValidContinuousQuery() {
		return fmt.Errorf("Continuous queries with a group by clause must include time(...) as one of the elements")
	}

	if !selectQuery.IsNonRecursiveContinuousQuery() {
		return fmt.Errorf("Continuous queries with :series_name interpolation must use a regular expression in the from clause that prevents recursion")
	}

	duration, err := selectQuery.GetGroupByClause().GetGroupByTime()
	if err != nil {
		return fmt.Errorf("Couldn't get group by time for continuous query: %s", err)
	}

	// if there are already-running queries, we need to initiate a backfill
	if selectQuery.GetIntoClause().Backfill &&
		!s.clusterConfig.LastContinuousQueryRunTime().IsZero() {
		zeroTime := time.Time{}
		currentBoundary := time.Now()
		if duration != nil {
			currentBoundary = currentBoundary.Truncate(*duration)
		}
		go s.runContinuousQuery(db, selectQuery, zeroTime, currentBoundary)
	} else {
		// TODO: make continuous queries backfill for queries that don't have a group by time
	}

	command := NewCreateContinuousQueryCommand(db, query)
	_, err = s.doOrProxyCommand(command)
	return err
}

func (s *RaftServer) DeleteContinuousQuery(db string, id uint32) error {
	command := NewDeleteContinuousQueryCommand(db, id)
	_, err := s.doOrProxyCommand(command)
	return err
}

func (s *RaftServer) ChangeConnectionString(raftName, protobufConnectionString, raftConnectionString string, forced bool) error {
	command := &InfluxChangeConnectionStringCommand{
		Force:                    true,
		Name:                     raftName,
		ConnectionString:         raftConnectionString,
		ProtobufConnectionString: protobufConnectionString,
	}
	for _, s := range s.raftServer.Peers() {
		// send the command and ignore errors in case a server is down
		SendCommandToServer(s.ConnectionString, command)
	}

	// make the change permament
	command.Force = false
	_, err := s.doOrProxyCommand(command)
	log.Info("Running the actual command")
	return err
}

func (s *RaftServer) AssignCoordinator(coordinator *CoordinatorImpl) error {
	s.coordinator = coordinator
	return nil
}

const (
	MAX_SIZE = 10 * MEGABYTE
)

func (s *RaftServer) ForceLogCompaction() error {
	err := s.raftServer.TakeSnapshot()
	if err != nil {
		log.Error("Cannot take snapshot: %s", err)
		return err
	}
	return nil
}

func (s *RaftServer) CompactLog() {
	checkSizeTicker := time.Tick(time.Minute)
	forceCompactionTicker := time.Tick(time.Hour * 24)

	for {
		select {
		case <-checkSizeTicker:
			log.Debug("Testing if we should compact the raft logs")

			path := s.raftServer.LogPath()
			size, err := common.GetFileSize(path)
			if err != nil {
				log.Error("Error getting size of file '%s': %s", path, err)
			}
			if size < MAX_SIZE {
				continue
			}
			s.ForceLogCompaction()
		case <-forceCompactionTicker:
			s.ForceLogCompaction()
		}
	}
}

func (s *RaftServer) CommittedAllChanges() bool {
	entries := s.raftServer.LogEntries()
	if len(entries) == 0 {
		if s.raftServer.CommitIndex() == 0 {
			// commit index should never be zero, we have at least one
			// raft join command that should be committed, something is
			// wrong, return false to be safe
			return false
		}
		// may be we recovered from a snapshot ?
		return true
	}
	lastIndex := entries[len(entries)-1].Index()
	return s.raftServer.CommitIndex() == lastIndex
}

func (s *RaftServer) startRaft() error {
	log.Info("Initializing Raft Server: %s", s.config.RaftConnectionString())

	// Initialize and start Raft server.
	transporter := raft.NewHTTPTransporter("/raft")
	var err error
	s.raftServer, err = raft.NewServer(s.name, s.path, transporter, s.clusterConfig, s.clusterConfig, "")
	if err != nil {
		return err
	}

	s.raftServer.SetElectionTimeout(s.config.RaftTimeout.Duration)
	s.raftServer.LoadSnapshot() // ignore errors

	s.raftServer.AddEventListener(raft.StateChangeEventType, s.raftEventHandler)

	transporter.Install(s.raftServer, s)
	s.raftServer.Start()

	go s.CompactLog()

	if !s.raftServer.IsLogEmpty() {
		log.Info("Recovered from log")
		return nil
	}

	potentialLeaders := s.config.SeedServers

	if len(potentialLeaders) == 0 {
		log.Info("Starting as new Raft leader...")
		name := s.raftServer.Name()
		_, err := s.raftServer.Do(&InfluxJoinCommand{
			Name:                     name,
			ConnectionString:         s.config.RaftConnectionString(),
			ProtobufConnectionString: s.config.ProtobufConnectionString(),
		})

		if err != nil {
			log.Error(err)
		}
		err = s.CreateRootUser()
		return err
	}

	for {
		for _, leader := range potentialLeaders {
			log.Info("(raft:%s) Attempting to join leader: %s", s.raftServer.Name(), leader)

			if err := s.Join(leader); err == nil {
				log.Info("Joined: %s", leader)
				return nil
			}
		}

		log.Warn("Couldn't join any of the seeds, sleeping and retrying...")
		time.Sleep(100 * time.Millisecond)
	}
}

func (s *RaftServer) raftEventHandler(e raft.Event) {
	if e.Value() == "leader" {
		log.Info("(raft:%s) Selected as leader. Starting leader loop.", s.raftServer.Name())
		config := s.clusterConfig.GetLocalConfiguration()
		retentionSweepPeriod := config.StorageRetentionSweepPeriod.Duration
		retentionSweepTimer := time.NewTicker(retentionSweepPeriod)
		go s.raftLeaderLoop(time.NewTicker(1*time.Second),
			retentionSweepTimer)
	}

	if e.PrevValue() == "leader" {
		log.Info("(raft:%s) Demoted from leader. Ending leader loop.", s.raftServer.Name())
		s.notLeader <- true
	}
}

func (s *RaftServer) raftLeaderLoop(loopTimer *time.Ticker,
	retentionSweepTimer *time.Ticker) {
	for {
		select {
		case <-loopTimer.C:
			log.Debug("(raft:%s) Executing leader loop.", s.raftServer.Name())
			s.checkContinuousQueries()
			break
		case <-retentionSweepTimer.C:
			s.dropShardsWithRetentionPolicies()
			break
		case <-s.notLeader:
			log.Debug("(raft:%s) Exiting leader loop.", s.raftServer.Name())
			return
		}
	}
}

func (s *RaftServer) StartProcessingContinuousQueries() {
	s.processContinuousQueries = true
}

func (s *RaftServer) checkContinuousQueries() {
	if !s.processContinuousQueries {
		return
	}

	if !s.clusterConfig.HasContinuousQueries() {
		return
	}

	runTime := time.Now()
	queriesDidRun := false

	for db, queries := range s.clusterConfig.ParsedContinuousQueries {
		for _, query := range queries {
			groupByClause := query.GetGroupByClause()

			// if there's no group by clause, it's handled as a fanout query
			if groupByClause.Elems == nil {
				continue
			}

			duration, err := query.GetGroupByClause().GetGroupByTime()
			if err != nil {
				log.Error("Couldn't get group by time for continuous query:", err)
				continue
			}

			currentBoundary := runTime.Truncate(*duration)
			lastRun := s.clusterConfig.LastContinuousQueryRunTime()
			lastBoundary := lastRun.Truncate(*duration)

			if currentBoundary.After(lastRun) {
				s.runContinuousQuery(db, query, lastBoundary, currentBoundary)
				queriesDidRun = true
			}
		}
	}

	if queriesDidRun {
		s.clusterConfig.SetLastContinuousQueryRunTime(runTime)
		s.SetContinuousQueryTimestamp(runTime)
	}
}

func (s *RaftServer) dropShardsWithRetentionPolicies() {
	log.Info("Checking for shards to drop")
	shards := s.clusterConfig.GetExpiredShards()
	for _, shard := range shards {
		s.DropShard(shard.Id(), shard.ServerIds())
	}
}

func (s *RaftServer) runContinuousQuery(db string, query *parser.SelectQuery, start time.Time, end time.Time) {
	adminName := s.clusterConfig.GetClusterAdmins()[0]
	clusterAdmin := s.clusterConfig.GetClusterAdmin(adminName)
	intoClause := query.GetIntoClause()
	targetName := intoClause.Target.Name
	queryString := query.GetQueryStringWithTimesAndNoIntoClause(start, end)

	f := func(series *protocol.Series) error {
		return s.coordinator.InterpolateValuesAndCommit(query.GetQueryString(), db, series, targetName, true)
	}

	writer := NewContinuousQueryWriter(f)
	s.coordinator.RunQuery(clusterAdmin, db, queryString, writer)
}

func (s *RaftServer) ListenAndServe() error {
	l, err := net.Listen("tcp", s.config.RaftListenString())
	if err != nil {
		panic(err)
	}
	return s.Serve(l)
}

func (s *RaftServer) Serve(l net.Listener) error {
	s.listener = l

	log.Info("Initializing Raft HTTP server")

	// Initialize and start HTTP server.
	s.httpServer = &http.Server{
		Handler: s.router,
	}

	s.router.HandleFunc("/cluster_config", s.configHandler).Methods("GET")
	s.router.HandleFunc("/join", s.joinHandler).Methods("POST")
	s.router.HandleFunc("/process_command/{command_type}", s.processCommandHandler).Methods("POST")

	log.Info("Raft Server Listening at %s", s.config.RaftListenString())

	go func() {
		err := s.httpServer.Serve(l)
		if !strings.Contains(err.Error(), "closed network") {
			panic(err)
		}
	}()
	started := make(chan error)
	go func() {
		started <- s.startRaft()
	}()
	err := <-started
	//	time.Sleep(3 * time.Second)
	return err
}

func (self *RaftServer) Close() {
	if !self.closing || self.raftServer == nil {
		self.closing = true
		self.raftServer.Stop()
		self.listener.Close()
		self.notLeader <- true
	}
}

// This is a hack around Gorilla mux not providing the correct net/http
// HandleFunc() interface.
func (s *RaftServer) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	s.router.HandleFunc(pattern, handler)
}

// Joins to the leader of an existing cluster.
func (s *RaftServer) RemoveServer(id uint32) error {
	command := &InfluxForceLeaveCommand{
		Id: id,
	}
	for _, s := range s.raftServer.Peers() {
		// send the command and ignore errors in case a server is down
		SendCommandToServer(s.ConnectionString, command)
	}

	if _, err := command.Apply(s.raftServer); err != nil {
		return err
	}

	// make the change permament
	log.Info("Running the actual command")
	_, err := s.doOrProxyCommand(command)
	return err
}

// Joins to the leader of an existing cluster.
func (s *RaftServer) Join(leader string) error {
	command := &InfluxJoinCommand{
		Name:                     s.raftServer.Name(),
		ConnectionString:         s.config.RaftConnectionString(),
		ProtobufConnectionString: s.config.ProtobufConnectionString(),
	}
	connectUrl := leader
	if !strings.HasPrefix(connectUrl, "http://") {
		connectUrl = "http://" + connectUrl
	}
	if !strings.HasSuffix(connectUrl, "/join") {
		connectUrl = connectUrl + "/join"
	}

	var b bytes.Buffer
	json.NewEncoder(&b).Encode(command)
	log.Debug("(raft:%s) Posting to seed server %s", s.raftServer.Name(), connectUrl)
	tr := &http.Transport{
		ResponseHeaderTimeout: time.Second,
	}
	client := &http.Client{Transport: tr}
	resp, err := client.Post(connectUrl, "application/json", &b)
	if err != nil {
		log.Error(err)
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTemporaryRedirect {
		address := resp.Header.Get("Location")
		log.Debug("Redirected to %s to join leader", address)
		return s.Join(address)
	}

	log.Debug("(raft:%s) Posted to seed server %s", s.raftServer.Name(), connectUrl)
	return nil
}

func (s *RaftServer) retryCommand(command raft.Command, retries int) (ret interface{}, err error) {
	for ; retries > 0; retries-- {
		ret, err = s.raftServer.Do(command)
		if err == nil {
			return ret, nil
		}
		time.Sleep(50 * time.Millisecond)
		log.Info("Retrying RAFT command...")
	}
	return
}

func (s *RaftServer) joinHandler(w http.ResponseWriter, req *http.Request) {
	// if this is the leader, process the command
	if s.raftServer.State() == raft.Leader {
		command := &InfluxJoinCommand{}
		if err := json.NewDecoder(req.Body).Decode(&command); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		log.Debug("ON RAFT LEADER - JOIN: %v", command)
		// during the test suite the join command will sometimes time out.. just retry a few times
		if _, err := s.raftServer.Do(command); err != nil {
			log.Error("Can't process %v: %s", command, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	leader, ok := s.leaderConnectString()
	log.Debug("Non-leader redirecting to: (%v, %v)", leader, ok)
	if ok {
		log.Debug("redirecting to leader to join...")
		http.Redirect(w, req, leader+"/join", http.StatusTemporaryRedirect)
	} else {
		http.Error(w, errors.New("Couldn't find leader of the cluster to join").Error(), http.StatusInternalServerError)
	}
}

func (s *RaftServer) configHandler(w http.ResponseWriter, req *http.Request) {
	js, err := json.Marshal(s.clusterConfig.GetMapForJsonSerialization())
	if err != nil {
		log.Error("ERROR marshalling config: ", err)
	}
	w.Write(js)
}

func (s *RaftServer) marshalAndDoCommandFromBody(command raft.Command, req *http.Request) (interface{}, error) {
	if err := json.NewDecoder(req.Body).Decode(&command); err != nil {
		return nil, err
	}

	if c, ok := command.(*InfluxChangeConnectionStringCommand); ok && c.Force {
		// if this is a forced change, just do it now and return. Note
		// that this isn't a permanent change, since on restart the old
		// connection strings will be used
		return c.Apply(s.raftServer)
	}

	if result, err := s.raftServer.Do(command); err != nil {
		return nil, err
	} else {
		return result, nil
	}
}

func (s *RaftServer) processCommandHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	value := vars["command_type"]
	command := internalRaftCommands[value]
	v := reflect.New(reflect.Indirect(reflect.ValueOf(command)).Type()).Interface()
	copy, ok := v.(raft.Command)
	if !ok {
		panic(fmt.Sprintf("raft: Unable to copy command: %s (%v)", command.CommandName(), reflect.ValueOf(v).Kind().String()))
	}

	if result, err := s.marshalAndDoCommandFromBody(copy, req); err != nil {
		log.Error("command %T failed: %s", copy, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else {
		if result != nil {
			js, _ := json.Marshal(result)
			w.Write(js)
		}
	}
}

func (self *RaftServer) CreateShards(shards []*cluster.NewShardData) ([]*cluster.ShardData, error) {
	log.Debug("RAFT: CreateShards")
	command := NewCreateShardsCommand(shards)
	createShardsResult, err := self.doOrProxyCommand(command)
	if err != nil {
		log.Error("RAFT: CreateShards: ", err)
		return nil, err
	}
	if x, k := createShardsResult.([]byte); k {
		newShards := make([]*cluster.NewShardData, 0)
		err = json.Unmarshal(x, &newShards)
		if err != nil {
			log.Error("RAFT: error parsing new shard result: ", err)
			return nil, err
		}
		return self.clusterConfig.MarshalNewShardArrayToShards(newShards)
	}
	if x, k := createShardsResult.([]*cluster.NewShardData); k {
		return self.clusterConfig.MarshalNewShardArrayToShards(x)
	}

	return nil, fmt.Errorf("Unable to marshal Raft AddShards result!")
}

func (self *RaftServer) DropShard(id uint32, serverIds []uint32) error {
	if self.clusterConfig.GetShard(id) == nil {
		log.Warn("Attempted to drop shard that doesn't exist: ", id)
		return nil
	}
	command := NewDropShardCommand(id, serverIds)
	_, err := self.doOrProxyCommand(command)
	return err
}

func (self *RaftServer) GetOrSetFieldIdsForSeries(database string, series []*protocol.Series) ([]*protocol.Series, error) {
	command := NewCreateSeriesFieldIdsCommand(database, series)
	result, err := self.doOrProxyCommand(command)
	if result == nil || err != nil {
		return nil, err
	}
	if x, k := result.([]byte); k {
		s := []*protocol.Series{}
		err := json.Unmarshal(x, &s)
		if err != nil {
			return nil, err
		}
		return s, nil
	}
	if x, k := result.([]*protocol.Series); k {
		return x, nil
	}
	return nil, nil
}

func (self *RaftServer) DropSeries(database, series string) error {
	command := NewDropSeriesCommand(database, series)
	_, err := self.doOrProxyCommand(command)

	// TODO: Dropping series from the metastore is synchronous, but the underlying data
	//       delete is asynchronous. If the server crashes or restarts while this is happening
	//       there will be orphaned data sitting around. Not a huge deal, but we should fix this
	//       at some point.
	// force a log compaction because we don't want this replaying after a server restarts
	if err == nil {
		err = self.ForceLogCompaction()
	}
	return err
}

func (self *RaftServer) CreateShardSpace(shardSpace *cluster.ShardSpace) error {
	command := NewCreateShardSpaceCommand(shardSpace)
	_, err := self.doOrProxyCommand(command)
	return err
}

func (self *RaftServer) DropShardSpace(database, name string) error {
	command := NewDropShardSpaceCommand(database, name)
	_, err := self.doOrProxyCommand(command)
	return err
}
