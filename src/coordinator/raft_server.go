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
	"sync"
	"time"
)

// The raftd server is a combination of the Raft server and an HTTP
// server which acts as the transport.
type RaftServer struct {
	name                        string
	host                        string
	port                        int
	path                        string
	router                      *mux.Router
	raftServer                  *raft.Server
	httpServer                  *http.Server
	clusterConfig               *ClusterConfiguration
	mutex                       sync.RWMutex
	listener                    net.Listener
	nameToConnectionStrings     map[string]string
	nameToConnectionStringsLock sync.RWMutex
}

var registeredCommands bool

// Creates a new server.
func NewRaftServer(path string, host string, port int, clusterConfig *ClusterConfiguration) *RaftServer {
	if !registeredCommands {
		//		raft.SetLogLevel(raft.Trace)
		registeredCommands = true
		raft.RegisterCommand(&AddApiKeyCommand{})
		raft.RegisterCommand(&RemoveApiKeyCommand{})
		raft.RegisterCommand(&AddServerToLocationCommand{})
		raft.RegisterCommand(&RemoveServerFromLocationCommand{})
	}
	s := &RaftServer{
		host:                    host,
		port:                    port,
		path:                    path,
		clusterConfig:           clusterConfig,
		router:                  mux.NewRouter(),
		nameToConnectionStrings: make(map[string]string),
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
	// s.nameToConnectionStringsLock.RLock()
	// defer s.nameToConnectionStringsLock.RUnlock()
	// l, ok := s.nameToConnectionStrings[leader]
	// return l, ok
}

func (s *RaftServer) proxyCommand(command raft.Command, commandType string) error {
	if leader, ok := s.leaderConnectString(); !ok {
		return errors.New("Couldn't connect to the cluster leader...")
	} else {
		var b bytes.Buffer
		json.NewEncoder(&b).Encode(command)
		resp, err := http.Post(leader+"/process_command/"+commandType, "application/json", &b)
		if err != nil {
			return err
		}
		resp.Body.Close()
	}
	return nil
}

func (s *RaftServer) AddReadApiKey(db, key string) error {
	if s.raftServer.State() == raft.Leader {
		_, err := s.raftServer.Do(NewAddApikeyCommand(db, key, ReadKey))
		return err
	} else {
		command := NewAddApikeyCommand(db, key, ReadKey)
		return s.proxyCommand(command, "add_api_key")
	}
}

func (s *RaftServer) AddWriteApiKey(db, key string) error {
	if s.raftServer.State() == raft.Leader {
		_, err := s.raftServer.Do(NewAddApikeyCommand(db, key, WriteKey))
		return err
	} else {
		command := NewAddApikeyCommand(db, key, WriteKey)
		return s.proxyCommand(command, "add_api_key")
	}
}

func (s *RaftServer) RemoveApiKey(db, key string) error {
	if s.raftServer.State() == raft.Leader {
		_, err := s.raftServer.Do(NewRemoveApiKeyCommand(db, key))
		return err
	} else {
		command := NewRemoveApiKeyCommand(db, key)
		return s.proxyCommand(command, "remove_api_key")
	}
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

	var b bytes.Buffer
	json.NewEncoder(&b).Encode(command)
	resp, err := http.Post(fmt.Sprintf("http://%s/join", leader), "application/json", &b)
	if err != nil {
		return err
	}
	resp.Body.Close()

	return nil
}

func (s *RaftServer) joinHandler(w http.ResponseWriter, req *http.Request) {
	command := &raft.DefaultJoinCommand{}

	if err := json.NewDecoder(req.Body).Decode(&command); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := s.raftServer.Do(command); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	} else {
		s.nameToConnectionStringsLock.Lock()
		defer s.nameToConnectionStringsLock.Unlock()
		log.Println("Adding: ", command.Name, command.ConnectionString)
		s.nameToConnectionStrings[command.Name] = command.ConnectionString
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
	js, err := json.Marshal(jsonObject)
	if err != nil {
		log.Println("ERROR marshalling config: ", err)
	}
	w.Write(js)
}

func (s *RaftServer) marshalAndDoCommandFromBody(command raft.Command, req *http.Request) error {
	log.Println("marshalAndDoCommand")
	if err := json.NewDecoder(req.Body).Decode(&command); err != nil {
		return err
	}
	if _, err := s.raftServer.Do(command); err != nil {
		return err
	}
	return nil
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
	}
	if err := s.marshalAndDoCommandFromBody(command, req); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
