package coordinator

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/influxdb/raft"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"path/filepath"
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
	clusterServer *ClusterServer
	mutex         sync.RWMutex
	listener      net.Listener
	closing       bool
}

var registeredCommands bool

// Creates a new server.
func NewRaftServer(path string, host string, port int, clusterConfig *ClusterConfiguration) *RaftServer {
	if !registeredCommands {
		registeredCommands = true
		raft.RegisterCommand(&AddPotentialServerCommand{})
		raft.RegisterCommand(&UpdateServerStateCommand{})
		raft.RegisterCommand(&CreateDatabaseCommand{})
		raft.RegisterCommand(&DropDatabaseCommand{})
		raft.RegisterCommand(&SaveDbUserCommand{})
		raft.RegisterCommand(&SaveClusterAdminCommand{})
	}
	s := &RaftServer{
		host:          host,
		port:          port,
		path:          path,
		clusterConfig: clusterConfig,
		router:        mux.NewRouter(),
	}
	// Read existing name or generate a new one.
	if b, err := ioutil.ReadFile(filepath.Join(path, "name")); err == nil {
		s.name = string(b)
	} else {
		s.name = fmt.Sprintf("%07x", rand.Int())[0:7]
		if err = ioutil.WriteFile(filepath.Join(path, "name"), []byte(s.name), 0644); err != nil {
			panic(err)
		}
	}

	return s
}

func (s *RaftServer) ClusterServer() *ClusterServer {
	if s.clusterServer != nil {
		return s.clusterServer
	}
	s.clusterServer = s.clusterConfig.GetServerByRaftName(s.name)
	return s.clusterServer
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
			fmt.Printf("Error: Cannot run command %#v. %s", command, err)
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

func (s *RaftServer) CreateDatabase(name string) error {
	command := NewCreateDatabaseCommand(name)
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

func (s *RaftServer) SaveClusterAdminUser(u *clusterAdmin) error {
	command := NewSaveClusterAdminCommand(u)
	_, err := s.doOrProxyCommand(command, "save_cluster_admin_user")
	return err
}

func (s *RaftServer) CreateRootUser() error {
	u := &clusterAdmin{CommonUser{"root", "", false}}
	u.changePassword(DEFAULT_ROOT_PWD)
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

func (s *RaftServer) startRaft(potentialLeaders []string, retryUntilJoin bool) {
	// there's a race condition in goraft that will cause the server to panic
	// while shutting down
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("Raft paniced: %v\n", err)
		}
	}()

	log.Printf("Initializing Raft Server: %s %d", s.path, s.port)

	// Initialize and start Raft server.
	transporter := raft.NewHTTPTransporter("/raft")
	var err error
	s.raftServer, err = raft.NewServer(s.name, s.path, transporter, nil, s.clusterConfig, "")
	if err != nil {
		log.Fatal(err)
	}

	transporter.Install(s.raftServer, s)
	s.raftServer.Start()

	if s.raftServer.IsLogEmpty() {
		for {
			joined := false
			for _, leader := range potentialLeaders {
				log.Println("Attempting to join leader: ", leader, s.port)

				if err := s.Join(leader); err == nil {
					joined = true
					log.Println("Joined: ", leader)
					break
				}
			}
			// couldn't join a leader so we must be the first one up
			if joined {
				break
			} else if !joined && !retryUntilJoin {
				log.Println("Couldn't contact a leader so initializing new cluster for server on port: ", s.port)

				name := s.raftServer.Name()
				connectionString := s.connectionString()
				_, err := s.raftServer.Do(&raft.DefaultJoinCommand{
					Name:             name,
					ConnectionString: connectionString,
				})
				if err != nil {
					log.Fatal(err)
				}

				command := NewAddPotentialServerCommand(&ClusterServer{RaftName: name, RaftConnectionString: connectionString})
				s.doOrProxyCommand(command, "add_server")
				s.CreateRootUser()
				break
			} else {
				// sleep for a little bit and retry it
				log.Println("Couldn't join any of the seeds, sleeping and retrying...")
				time.Sleep(100 * time.Millisecond)
			}
		}
	} else {
		log.Println("Recovered from log")
	}
}

func (s *RaftServer) ListenAndServe(potentialLeaders []string, retryUntilJoin bool) error {
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		panic(err)
	}
	return s.Serve(l, potentialLeaders, retryUntilJoin)
}

func (s *RaftServer) Serve(l net.Listener, potentialLeaders []string, retryUntilJoin bool) error {
	s.port = l.Addr().(*net.TCPAddr).Port
	s.listener = l

	go s.startRaft(potentialLeaders, retryUntilJoin)

	log.Println("Initializing Raft HTTP server")

	// Initialize and start HTTP server.
	s.httpServer = &http.Server{
		Handler: s.router,
	}

	s.router.HandleFunc("/cluster_config", s.configHandler).Methods("GET")
	s.router.HandleFunc("/join", s.joinHandler).Methods("POST")
	s.router.HandleFunc("/process_command/{command_type}", s.processCommandHandler).Methods("POST")

	log.Println("Listening at:", s.connectionString())

	return s.httpServer.Serve(l)
}

func (self *RaftServer) Close() {
	if !self.closing {
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
	command := &raft.DefaultJoinCommand{
		Name:             s.raftServer.Name(),
		ConnectionString: s.connectionString(),
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
	resp, err := http.Post(connectUrl, "application/json", &b)
	if err != nil {
		log.Println("ERROR: ", err)
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTemporaryRedirect {
		address := resp.Header.Get("Location")
		log.Printf("Redirected to %s to join leader\n", address)
		return s.Join(address)
	}

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
		command := &raft.DefaultJoinCommand{}
		if err := json.NewDecoder(req.Body).Decode(&command); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		// during the test suite the join command will sometimes time out.. just retry a few times
		if _, err := s.raftServer.Do(command); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		server := s.clusterConfig.GetServerByRaftName(command.Name)
		// it's a new server the cluster has never seen, make it a potential
		if server == nil {
			addServer := NewAddPotentialServerCommand(&ClusterServer{RaftName: command.Name, RaftConnectionString: command.ConnectionString})
			if _, err := s.raftServer.Do(addServer); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
	} else {
		if leader, ok := s.leaderConnectString(); ok {
			log.Println("redirecting to leader to join...")
			http.Redirect(w, req, leader+"/join", http.StatusTemporaryRedirect)
		} else {
			http.Error(w, errors.New("Couldn't find leader of the cluster to join").Error(), http.StatusInternalServerError)
		}
	}
}

func (s *RaftServer) configHandler(w http.ResponseWriter, req *http.Request) {
	jsonObject := make(map[string]interface{})
	dbs := make([]string, 0)
	for db, _ := range s.clusterConfig.databaseNames {
		dbs = append(dbs, db)
	}
	jsonObject["databases"] = dbs
	jsonObject["cluster_admins"] = s.clusterConfig.clusterAdmins
	jsonObject["database_users"] = s.clusterConfig.dbUsers
	js, err := json.Marshal(jsonObject)
	if err != nil {
		log.Println("ERROR marshalling config: ", err)
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
	var command raft.Command
	if value == "create_db" {
		command = &CreateDatabaseCommand{}
	} else if value == "drop_db" {
		command = &DropDatabaseCommand{}
	} else if value == "save_db_user" {
		command = &SaveDbUserCommand{}
	} else if value == "save_cluster_admin_user" {
		command = &SaveClusterAdminCommand{}
	} else if value == "update_state" {
		command = &UpdateServerStateCommand{}
	} else if value == "add_server" {
		fmt.Println("add_server: ", s.name)
		command = &AddPotentialServerCommand{}
	}
	if result, err := s.marshalAndDoCommandFromBody(command, req); err != nil {
		log.Println("ERROR processCommandHanlder", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else {
		if result != nil {
			js, _ := json.Marshal(result)
			w.Write(js)
		}
	}
}
