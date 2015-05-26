package tsdb

import (
	"sync"

	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
)

func NewStore(path string) *Store {
	return &Store{
		path:   path,
		Logger: log.New(os.Stderr, "[node] ", log.LstdFlags),
	}
}

var (
	ErrShardNotFound = fmt.Errorf("shard not found")
)

type Store struct {
	path string

	mu sync.RWMutex

	databaseIndexes map[string]*DatabaseIndex
	shards          map[uint64]*Shard

	Logger *log.Logger
}

func (s *Store) CreateShard(database, replicationPolicy string, shardID uint64) error {
	// TODO: create database dir, replicaton policy dir, add shard to shards map and open
	return nil
}

func (s *Store) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.shards = map[uint64]*Shard{}
	s.databaseIndexes = map[string]*DatabaseIndex{}

	// Open shards
	// Start AE for Node

	// FIXME: Need to use config data dir
	path, err := ioutil.TempDir("", "influxdb-alpha1")
	if err != nil {
		return err
	}
	s.path = filepath.Join(path, "1")

	// TODO: there should be a separate database index for each database dir in the data directory
	s.databaseIndexes["fixthis"] = NewDatabaseIndex()
	shard := NewShard(s.databaseIndexes["fixthis"], s.path)

	if err := shard.Open(); err != nil {
		return err
	}
	s.Logger.Printf("opened temp shard at %s", s.path)

	s.shards[uint64(1)] = shard

	return nil
}

func (s *Store) WriteToShard(shardID uint64, points []Point) error {
	//TODO: Find the Shard for shardID
	//TODO: Write points to the shard
	sh, ok := s.shards[shardID]
	if !ok {
		return ErrShardNotFound
	}
	fmt.Printf("> WriteShard %d, %d points\n", shardID, len(points))

	if err := sh.WritePoints(points); err != nil {
		return err
	}
	return nil

}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, sh := range s.shards {
		if err := sh.Close(); err != nil {
			return err
		}
	}
	s.shards = nil
	s.databaseIndexes = nil

	return nil
}
