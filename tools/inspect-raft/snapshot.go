package main

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"os"

	"github.com/influxdb/influxdb/cluster"
)

type Peer struct {
	Name             string `json:"name"`
	ConnectionString string `json:"connectionString"`
}

type Snapshot struct {
	LastIndex uint64 `json:"lastIndex"`
	LastTerm  uint64 `json:"lastTerm"`

	// Cluster configuration.
	Peers []*Peer `json:"peers"`
	State []byte  `json:"state"`
	state *cluster.SavedConfiguration
	Path  string `json:"path"`
}

func (s *Snapshot) Load(path string) error {
	// Open the file for writing.
	file, err := os.OpenFile(path, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer file.Close()

	var checksum uint32
	_, err = fmt.Scanf("%08x\n", &checksum)
	if err != nil {
		return err
	}

	b, err := ioutil.ReadAll(file)

	// Serialize to JSON.
	err = json.Unmarshal(b, s)
	if err != nil {
		return err
	}

	s.state = &cluster.SavedConfiguration{}
	err = json.Unmarshal(s.State, s.state)
	if err != nil {
		return err
	}
	return nil
}

func (s *Snapshot) Save(path string) error {
	b, err := json.Marshal(s.state)
	if err != nil {
		return err
	}
	s.State = b

	b, err = json.Marshal(s.state)
	if err != nil {
		return err
	}

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	defer file.Close()

	// Generate checksum and write it to disk.
	checksum := crc32.ChecksumIEEE(b)
	if _, err := fmt.Fprintf(file, "%08x\n", checksum); err != nil {
		return err
	}

	// Write the snapshot to disk.
	if _, err = file.Write(b); err != nil {
		return err
	}

	return nil
}
