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

func setupLog(content string) string {
	f, _ := ioutil.TempFile("", "raft-log-")
	f.Write([]byte(content))
	f.Close()
	return f.Name()
}

//--------------------------------------
// Servers
//--------------------------------------

func newTestServer(name string) *Server {
	path, _ := ioutil.TempDir("", "raft-server-")
	server, _ := NewServer(name, path)
	server.AddCommandType(&TestCommand1{})
	server.AddCommandType(&TestCommand2{})
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
