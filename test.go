package raft

import (
	"fmt"
	"io/ioutil"
	"os"
		"time"
)

const (
	testHeartbeatTimeout = 20 * time.Millisecond
	testElectionTimeout  = 60 * time.Millisecond
)

func init() {
	RegisterCommand(&joinCommand{})
	RegisterCommand(&TestCommand1{})
	RegisterCommand(&TestCommand2{})
}

//------------------------------------------------------------------------------
//
// Helpers
//
//------------------------------------------------------------------------------

//--------------------------------------
// Logs
//--------------------------------------

func getLogPath() string {
	f, _ := ioutil.TempFile("", "raft-log-")
	f.Close()
	os.Remove(f.Name())
	return f.Name()
}

func setupLogFile(content string) string {
	f, _ := ioutil.TempFile("", "raft-log-")
	f.Write([]byte(content))
	f.Close()
	return f.Name()
}

func setupLog(content string) (*Log, string) {
	path := setupLogFile(content)
	log := NewLog()
	log.ApplyFunc = func(c Command) error {
		return nil
	}
	if err := log.Open(path); err != nil {
		panic("Unable to open log")
	}
	return log, path
}

//--------------------------------------
// Servers
//--------------------------------------

func newTestServer(name string, transporter Transporter) *Server {
	path, _ := ioutil.TempDir("", "raft-server-")
	server, _ := NewServer(name, path, transporter, nil)
	return server
}

func newTestServerWithLog(name string, transporter Transporter, content string) *Server {
	server := newTestServer(name, transporter)
	ioutil.WriteFile(server.LogPath(), []byte(content), 0644)
	return server
}

func newTestCluster(names []string, transporter Transporter, lookup map[string]*Server) []*Server {
	servers := []*Server{}
	for _, name := range names {
		if lookup[name] != nil {
			panic(fmt.Sprintf("Duplicate server in test cluster! %v", name))
		}
		server := newTestServer(name, transporter)
		server.SetElectionTimeout(testElectionTimeout)
		servers = append(servers, server)
		lookup[name] = server
	}
	for _, server := range servers {
		server.SetHeartbeatTimeout(testHeartbeatTimeout)
		for _, peer := range servers {
			server.AddPeer(peer.Name())
		}
		server.Start()
	}
	return servers
}

//--------------------------------------
// Transporter
//--------------------------------------

type testTransporter struct {
    sendVoteRequestFunc func(server *Server, peer *Peer, req *RequestVoteRequest) (*RequestVoteResponse, error)
    sendAppendEntriesRequestFunc func(server *Server, peer *Peer, req *AppendEntriesRequest) (*AppendEntriesResponse, error)
    sendSnapshotRequestFunc func(server *Server, peer *Peer, req *SnapshotRequest) (*SnapshotResponse, error)
}

func (t *testTransporter) SendVoteRequest(server *Server, peer *Peer, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	return t.sendVoteRequestFunc(server, peer, req)
}

func (t *testTransporter) SendAppendEntriesRequest(server *Server, peer *Peer, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	return t.sendAppendEntriesRequestFunc(server, peer, req)
}

func (t *testTransporter) SendSnapshotRequest(server *Server, peer *Peer, req *SnapshotRequest) (*SnapshotResponse, error) {
	return t.sendSnapshotRequestFunc(server, peer, req)
} 


type testStateMachine struct {
	saveFunc func() ([]byte, error)
	recoveryFunc func([]byte) error
}

func (sm *testStateMachine) Save() ([]byte, error) {
	return sm.saveFunc()
}

func (sm *testStateMachine) Recovery(state []byte) error {
	return sm.recoveryFunc(state)
}


//--------------------------------------
// Join Command
//--------------------------------------

type joinCommand struct {
	Name string `json:"name"`
}

func (c *joinCommand) CommandName() string {
	return "test:join"
}

func (c *joinCommand) Apply(server *Server) error {
	err := server.AddPeer(c.Name)
	return err
}

//--------------------------------------
// Command1
//--------------------------------------

type TestCommand1 struct {
	Val string `json:"val"`
	I   int    `json:"i"`
}

func (c TestCommand1) CommandName() string {
	return "cmd_1"
}

func (c TestCommand1) Apply(server *Server) error {
	return nil
}

//--------------------------------------
// Command2
//--------------------------------------

type TestCommand2 struct {
	X int `json:"x"`
}

func (c TestCommand2) CommandName() string {
	return "cmd_2"
}

func (c TestCommand2) Apply(server *Server) error {
	return nil
}
