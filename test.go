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
	log.ApplyFunc = func(c Command) {}
	if err := log.Open(path); err != nil {
		panic("Unable to open log")
	}
	return log, path
}

//--------------------------------------
// Servers
//--------------------------------------

func newTestServer(name string) *Server {
	path, _ := ioutil.TempDir("", "raft-server-")
	server, _ := NewServer(name, path)
	return server
}

func newTestServerWithLog(name string, content string) *Server {
	server := newTestServer(name)
	ioutil.WriteFile(server.LogPath(), []byte(content), 0644)
	return server
}

func newTestCluster(names []string) (Servers, map[string]*Server) {
	servers := make(Servers, 0)
	lookup := make(map[string]*Server, 0)
	for _, name := range names {
		if lookup[name] != nil {
			panic(fmt.Sprintf("Duplicate server in test cluster! %v", name))
		}
		server := newTestServer(name)
		server.SetElectionTimeout(testElectionTimeout)
		servers = append(servers, server)
		lookup[name] = server
	}
	for _, server := range servers {
		for _, peer := range servers {
			if server != peer {
				server.peers[peer.Name()] = NewPeer(server, peer.Name(), testHeartbeatTimeout)
			}
		}
		server.Start()
	}
	return servers, lookup
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

func (c TestCommand1) Validate(server *Server) error {
	return nil
}

func (c TestCommand1) Apply(server *Server) {
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

func (c TestCommand2) Validate(server *Server) error {
	return nil
}

func (c TestCommand2) Apply(server *Server) {
}
