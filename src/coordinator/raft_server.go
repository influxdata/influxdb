package coordinator

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/goraft/raft"
	"github.com/gorilla/mux"
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

// The raftd server is a combination of the Raft server and an HTTP
// server which acts as the transport.
type RaftServer struct {
	name          string
	host          string
	port          int
	path          string
	router        *mux.Router
	raftServer    *raft.Server
	httpServer    *http.Server
	clusterConfig *ClusterConfiguration
	mutex         sync.RWMutex
	listener      net.Listener
}

var registeredCommands bool

// Creates a new server.
func NewRaftServer(path string, host string, port int, clusterConfig *ClusterConfiguration) *RaftServer {
	if !registeredCommands {
		// raft.SetLogLevel(raft.Trace)
		registeredCommands = true
		raft.RegisterCommand(&AddApiKeyCommand{})
		raft.RegisterCommand(&RemoveApiKeyCommand{})
		raft.RegisterCommand(&AddServerToLocationCommand{})
		raft.RegisterCommand(&RemoveServerFromLocationCommand{})
		raft.RegisterCommand(&NextDatabaseIdCommand{})
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
		ret, err := s.raftServer.Do(command)
		return ret, err
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
			var js interface{}
			json.Unmarshal(body, &js)
			return js, err2
		}
	}
	return nil, nil
}

func (s *RaftServer) AddReadApiKey(db, key string) error {
	command := NewAddApikeyCommand(db, key, ReadKey)
	_, err := s.doOrProxyCommand(command, "add_api_key")
	return err
}

func (s *RaftServer) AddWriteApiKey(db, key string) error {
	command := NewAddApikeyCommand(db, key, WriteKey)
	_, err := s.doOrProxyCommand(command, "add_api_key")
	return err
}

func (s *RaftServer) RemoveApiKey(db, key string) error {
	command := NewRemoveApiKeyCommand(db, key)
	_, err := s.doOrProxyCommand(command, "remove_api_key")
	return err
}

func (s *RaftServer) AddServerToLocation(host string, location int64) error {
	command := NewAddServerToLocationCommand(host, location)
	_, err := s.doOrProxyCommand(command, "add_server")
	return err
}

func (s *RaftServer) RemoveServerFromLocation(host string, location int64) error {
	command := NewRemoveServerFromLocationCommand(host, location)
	_, err := s.doOrProxyCommand(command, "remove_server")
	return err
}

func (s *RaftServer) GetNextDatabaseId() (string, error) {
	command := NewNextDatabaseIdCommand(s.clusterConfig.nextDatabaseId)
	id, err := s.doOrProxyCommand(command, "next_db")
	return id.(string), err
}

func (s *RaftServer) connectionString() string {
	return fmt.Sprintf("http://%s:%d", s.host, s.port)
}

func (s *RaftServer) ListenAndServe(potentialLeaders []string, retryUntilJoin bool) error {
	var err error

	log.Printf("Initializing Raft Server: %s %d", s.path, s.port)

	// Initialize and start Raft server.
	transporter := raft.NewHTTPTransporter("/raft")
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

				_, err := s.raftServer.Do(&raft.DefaultJoinCommand{
					Name:             s.raftServer.Name(),
					ConnectionString: s.connectionString(),
				})
				if err != nil {
					log.Fatal(err)
				}
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

	log.Println("Initializing Raft HTTP server")

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return err
	}
	// Initialize and start HTTP server.
	s.httpServer = &http.Server{
		Handler: s.router,
	}

	s.router.HandleFunc("/cluster_config", s.configHandler).Methods("GET")
	s.router.HandleFunc("/join", s.joinHandler).Methods("POST")
	s.router.HandleFunc("/process_command/{command_type}", s.processCommandHandler).Methods("POST")

	log.Println("Listening at:", s.connectionString())

	s.listener = l
	return s.httpServer.Serve(l)
}

func (self *RaftServer) Close() {
	self.raftServer.Stop()
	self.listener.Close()
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

func (s *RaftServer) joinHandler(w http.ResponseWriter, req *http.Request) {
	if s.raftServer.State() == raft.Leader {
		command := &raft.DefaultJoinCommand{}

		if err := json.NewDecoder(req.Body).Decode(&command); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if _, err := s.raftServer.Do(command); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
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
	readKeys := make([]string, 0)
	for k, _ := range s.clusterConfig.ReadApiKeys {
		readKeys = append(readKeys, k)
	}
	jsonObject["read_keys"] = readKeys
	writeKeys := make([]string, 0)
	for k, _ := range s.clusterConfig.WriteApiKeys {
		writeKeys = append(writeKeys, k)
	}
	jsonObject["write_keys"] = writeKeys
	locations := make([]map[string]interface{}, 0)
	for location, servers := range s.clusterConfig.RingLocationToServers {
		s := servers
		locations = append(locations, map[string]interface{}{"location": location, "servers": s})
	}
	jsonObject["locations"] = locations

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
	if value == "add_api_key" {
		command = &AddApiKeyCommand{}
	} else if value == "remove_api_key" {
		command = &RemoveApiKeyCommand{}
	} else if value == "add_server" {
		command = &AddServerToLocationCommand{}
	} else if value == "remove_server" {
		command = &RemoveServerFromLocationCommand{}
	} else if value == "next_db" {
		command = &NextDatabaseIdCommand{}
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
