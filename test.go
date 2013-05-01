package raft

import (
	"io/ioutil"
	"os"
)

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
	log.AddCommandType(&TestCommand1{})
	log.AddCommandType(&TestCommand2{})
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
	server.ApplyFunc = func(s *Server, c Command) {}
	server.AddCommandType(&TestCommand1{})
	server.AddCommandType(&TestCommand2{})
	return server
}

func newTestServerWithLog(name string, content string) *Server {
	server := newTestServer(name)
	ioutil.WriteFile(server.LogPath(), []byte(content), 0644)
	return server
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
