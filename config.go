package raft

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
)

type RaftConfig struct {
	KnownCommitIndex uint64 `json:"KnownCommitIndex"`
	// TODO decide what we need to store in peer struct
	Peers []string `json:"Peers"`
}

func (s *Server) writeConf() {

	peers := make([]string, len(s.peers))

	i := 0
	for peer := range s.peers {
		peers[i] = peer
		i++
	}

	r := &RaftConfig{
		KnownCommitIndex: s.log.commitIndex,
		Peers:            peers,
	}

	b, _ := json.Marshal(r)

	confBakPath := path.Join(s.path, "conf.bak")
	confPath := path.Join(s.path, "conf")

	confFile, err := os.OpenFile(confBakPath, os.O_WRONLY|os.O_CREATE, 0600)

	if err != nil {
		panic(err)
	}

	confFile.Write(b)

	os.Remove(confPath)
	os.Rename(confBakPath, confPath)
}

// Read the configuration for the server.
func (s *Server) readConf() error {
	confPath := path.Join(s.path, "conf")
	s.debugln("readConf.open ", confPath)
	// open conf file
	confFile, err := os.OpenFile(confPath, os.O_RDWR, 0600)

	if err != nil {
		if os.IsNotExist(err) {
			_, err = os.OpenFile(confPath, os.O_WRONLY|os.O_CREATE, 0600)
			debugln("readConf.create ", confPath)
			if err != nil {
				return err
			}
		}
		return err
	}

	raftConf := &RaftConfig{}

	b, err := ioutil.ReadAll(confFile)
	if err != nil {
		return err
	}

	confFile.Close()

	err = json.Unmarshal(b, raftConf)

	if err != nil {
		return err
	}

	s.log.commitIndex = raftConf.KnownCommitIndex

	return nil
}
