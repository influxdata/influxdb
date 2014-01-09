package coordinator

import (
	"bytes"
	log "code.google.com/p/log4go"
	"configuration"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/goraft/raft"
	"github.com/gorilla/mux"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"protocol"
	"strings"
	"sync"
	"time"
)

const (
	DEFAULT_ROOT_PWD = "root"
)

// The raftd server is a combination of the Raft server and an HTTP
// server which acts as the transport.
type RaftServer struct {
	name          string
	host          string
	port          int
	path          string
	router        *mux.Router
	raftServer    raft.Server
	httpServer    *http.Server
	clusterConfig *ClusterConfiguration
	mutex         sync.RWMutex
	listener      net.Listener
	closing       bool
	config        *configuration.Configuration
}

var registeredCommands bool
var replicateWrite = protocol.Request_REPLICATION_WRITE
var replicateDelete = protocol.Request_REPLICATION_DELETE

// Creates a new server.
func NewRaftServer(config *configuration.Configuration, clusterConfig *ClusterConfiguration) *RaftServer {
	if !registeredCommands {
		registeredCommands = true
		for _, command := range internalRaftCommands {
			raft.RegisterCommand(command)
		}
	}

	s := &RaftServer{
		host:          config.HostnameOrDetect(),
		port:          config.RaftServerPort,
		path:          config.RaftDir,
		clusterConfig: clusterConfig,
		router:        mux.NewRouter(),
		config:        config,
	}
	// Read existing name or generate a new one.
	if b, err := ioutil.ReadFile(filepath.Join(s.path, "name")); err == nil {
		s.name = string(b)
	} else {
		var i uint64
		if _, err := os.Stat("/dev/random"); err == nil {
			log.Info("Using /dev/random to initialize the raft server name")
			f, err := os.Open("/dev/random")
			if err != nil {
				panic(err)
			}
			b := make([]byte, 8)
			_, err = f.Read(b)
			if err != nil {
				panic(err)
			}
			i, err = binary.ReadUvarint(bytes.NewBuffer(b))
			if err != nil {
				panic(err)
			}
		} else {
			log.Info("Using rand package to generate raft server name")
			rand.Seed(time.Now().UnixNano())
			i = uint64(rand.Int())
		}
		s.name = fmt.Sprintf("%07x", i)[0:7]
		log.Info("Setting raft name to %s", s.name)
		if err = ioutil.WriteFile(filepath.Join(s.path, "name"), []byte(s.name), 0644); err != nil {
			panic(err)
		}
	}

	return s
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

func (s *RaftServer) doOrProxyCommand(command raft.Command, commandType string) (interface{}, error) {
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
			var b bytes.Buffer
			json.NewEncoder(&b).Encode(command)
			resp, err := http.Post(leader+"/process_command/"+commandType, "application/json", &b)
			if err != nil {
				return nil, err
			}
			defer resp.Body.Close()
			body, err2 := ioutil.ReadAll(resp.Body)

			if resp.StatusCode != 200 {
				return nil, errors.New(strings.TrimSpace(string(body)))
			}

			var js interface{}
			json.Unmarshal(body, &js)
			return js, err2
		}
	}
	return nil, nil
}

func (s *RaftServer) CreateDatabase(name string, replicationFactor uint8) error {
	if replicationFactor == 0 {
		replicationFactor = 1
	}
	command := NewCreateDatabaseCommand(name, replicationFactor)
	_, err := s.doOrProxyCommand(command, "create_db")
	return err
}

func (s *RaftServer) DropDatabase(name string) error {
	command := NewDropDatabaseCommand(name)
	_, err := s.doOrProxyCommand(command, "drop_db")
	return err
}

func (s *RaftServer) SaveDbUser(u *dbUser) error {
	command := NewSaveDbUserCommand(u)
	_, err := s.doOrProxyCommand(command, "save_db_user")
	return err
}

func (s *RaftServer) ChangeDbUserPassword(db, username string, hash []byte) error {
	command := NewChangeDbUserPasswordCommand(db, username, string(hash))
	_, err := s.doOrProxyCommand(command, "change_db_user_password")
	return err
}

func (s *RaftServer) SaveClusterAdminUser(u *clusterAdmin) error {
	command := NewSaveClusterAdminCommand(u)
	_, err := s.doOrProxyCommand(command, "save_cluster_admin_user")
	return err
}

func (s *RaftServer) CreateRootUser() error {
	u := &clusterAdmin{CommonUser{"root", "", false}}
	hash, _ := hashPassword(DEFAULT_ROOT_PWD)
	u.changePassword(string(hash))
	return s.SaveClusterAdminUser(u)
}

func (s *RaftServer) ActivateServer(server *ClusterServer) error {
	return errors.New("not implemented")
}

func (s *RaftServer) AddServer(server *ClusterServer, insertIndex int) error {
	return errors.New("not implemented")
}

func (s *RaftServer) MovePotentialServer(server *ClusterServer, insertIndex int) error {
	return errors.New("not implemented")
}

func (s *RaftServer) ReplaceServer(oldServer *ClusterServer, replacement *ClusterServer) error {
	return errors.New("not implemented")
}

func (s *RaftServer) connectionString() string {
	return fmt.Sprintf("http://%s:%d", s.host, s.port)
}

func (s *RaftServer) startRaft() error {
	log.Info("Initializing Raft Server: %s %d", s.path, s.port)

	// Initialize and start Raft server.
	transporter := raft.NewHTTPTransporter("/raft")
	var err error
	s.raftServer, err = raft.NewServer(s.name, s.path, transporter, nil, s.clusterConfig, "")
	if err != nil {
		return err
	}

	transporter.Install(s.raftServer, s)
	s.raftServer.Start()

	if !s.raftServer.IsLogEmpty() {
		log.Info("Recovered from log")
		return nil
	}

	potentialLeaders := s.config.SeedServers

	if len(potentialLeaders) == 0 {
		log.Info("Starting as new Raft leader...")
		name := s.raftServer.Name()
		connectionString := s.connectionString()
		_, err := s.raftServer.Do(&InfluxJoinCommand{
			Name:                     name,
			ConnectionString:         connectionString,
			ProtobufConnectionString: s.config.ProtobufConnectionString(),
		})

		if err != nil {
			log.Error(err)
		}

		command := NewAddPotentialServerCommand(&ClusterServer{
			RaftName:                 name,
			RaftConnectionString:     connectionString,
			ProtobufConnectionString: s.config.ProtobufConnectionString(),
		})
		_, err = s.doOrProxyCommand(command, "add_server")
		if err != nil {
			return err
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
	return nil
}

func (s *RaftServer) ListenAndServe() error {
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		panic(err)
	}
	return s.Serve(l)
}

func (s *RaftServer) Serve(l net.Listener) error {
	s.port = l.Addr().(*net.TCPAddr).Port
	s.listener = l

	log.Info("Initializing Raft HTTP server")

	// Initialize and start HTTP server.
	s.httpServer = &http.Server{
		Handler: s.router,
	}

	s.router.HandleFunc("/cluster_config", s.configHandler).Methods("GET")
	s.router.HandleFunc("/join", s.joinHandler).Methods("POST")
	s.router.HandleFunc("/process_command/{command_type}", s.processCommandHandler).Methods("POST")

	log.Info("Raft Server Listening at %s", s.connectionString())

	go func() {
		s.httpServer.Serve(l)
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
	}
}

// This is a hack around Gorilla mux not providing the correct net/http
// HandleFunc() interface.
func (s *RaftServer) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	s.router.HandleFunc(pattern, handler)
}

// Joins to the leader of an existing cluster.
func (s *RaftServer) Join(leader string) error {
	command := &InfluxJoinCommand{
		Name:                     s.raftServer.Name(),
		ConnectionString:         s.connectionString(),
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
		log.Debug("Redirected to %s to join leader\n", address)
		return s.Join(address)
	}

	log.Debug("(raft:%s) Posted to seed server %s", s.raftServer.Name(), connectUrl)
	return nil
}

func (s *RaftServer) retryCommand(command raft.Command, retries int) (ret interface{}, err error) {
	for retries = retries; retries > 0; retries-- {
		ret, err = s.raftServer.Do(command)
		if err == nil {
			return ret, nil
		}
		time.Sleep(50 * time.Millisecond)
		fmt.Println("Retrying RAFT command...")
	}
	return
}

func (s *RaftServer) joinHandler(w http.ResponseWriter, req *http.Request) {
	if s.raftServer.State() == raft.Leader {
		command := &InfluxJoinCommand{}
		if err := json.NewDecoder(req.Body).Decode(&command); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		log.Debug("Leader processing: %v", command)
		// during the test suite the join command will sometimes time out.. just retry a few times
		if _, err := s.raftServer.Do(command); err != nil {
			log.Error("Can't process %v: %s", command, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		server := s.clusterConfig.GetServerByRaftName(command.Name)
		// it's a new server the cluster has never seen, make it a potential
		if server == nil {
			log.Info("Adding new server to the cluster config %s", command.Name)
			addServer := NewAddPotentialServerCommand(&ClusterServer{
				RaftName:                 command.Name,
				RaftConnectionString:     command.ConnectionString,
				ProtobufConnectionString: command.ProtobufConnectionString,
			})
			if _, err := s.raftServer.Do(addServer); err != nil {
				log.Error("Error joining raft server: ", err, command)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
		log.Info("Server %s already exist in the cluster config", command.Name)
	} else {
		leader, ok := s.leaderConnectString()
		log.Debug("Non-leader redirecting to: (%v, %v)", leader, ok)
		if ok {
			log.Debug("redirecting to leader to join...")
			http.Redirect(w, req, leader+"/join", http.StatusTemporaryRedirect)
		} else {
			http.Error(w, errors.New("Couldn't find leader of the cluster to join").Error(), http.StatusInternalServerError)
		}
	}
}

func (s *RaftServer) configHandler(w http.ResponseWriter, req *http.Request) {
	jsonObject := make(map[string]interface{})
	dbs := make([]string, 0)
	for db, _ := range s.clusterConfig.databaseReplicationFactors {
		dbs = append(dbs, db)
	}
	jsonObject["databases"] = dbs
	jsonObject["cluster_admins"] = s.clusterConfig.clusterAdmins
	jsonObject["database_users"] = s.clusterConfig.dbUsers
	js, err := json.Marshal(jsonObject)
	if err != nil {
		log.Error("ERROR marshalling config: ", err)
	}
	w.Write(js)
}

func (s *RaftServer) marshalAndDoCommandFromBody(command raft.Command, req *http.Request) (interface{}, error) {
	if err := json.NewDecoder(req.Body).Decode(&command); err != nil {
		return nil, err
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

	if result, err := s.marshalAndDoCommandFromBody(command, req); err != nil {
		log.Error("command %T failed: %s", command, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else {
		if result != nil {
			js, _ := json.Marshal(result)
			w.Write(js)
		}
	}
}
