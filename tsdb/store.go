package tsdb

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/influxdb/influxdb/influxql"
)

func NewStore(path string) *Store {
	return &Store{
		path:   path,
		Logger: log.New(os.Stderr, "[store] ", log.LstdFlags),
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

func (s *Store) CreateShard(database, retentionPolicy string, shardID uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// shard already exists
	if _, ok := s.shards[shardID]; ok {
		return nil
	}

	// created the db and retention policy dirs if they don't exist
	if err := os.MkdirAll(filepath.Join(s.path, database, retentionPolicy), 0700); err != nil {
		return err
	}

	// create the database index if it does not exist
	db, ok := s.databaseIndexes[database]
	if !ok {
		db = NewDatabaseIndex()
		s.databaseIndexes[database] = db
	}

	shardPath := filepath.Join(s.path, database, retentionPolicy, strconv.FormatUint(shardID, 10))
	shard := NewShard(db, shardPath)

	s.shards[shardID] = shard

	return nil
}

func (s *Store) Shard(shardID uint64) *Shard {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.shards[shardID]
}

func (s *Store) ValidateAggregateFieldsInStatement(shardID uint64, measurementName string, stmt *influxql.SelectStatement) error {
	s.mu.RLock()
	shard := s.shards[shardID]
	s.mu.RUnlock()
	if shard == nil {
		return ErrShardNotFound
	}
	return shard.ValidateAggregateFieldsInStatement(measurementName, stmt)
}

func (s *Store) DatabaseIndex(name string) *DatabaseIndex {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.databaseIndexes[name]
}

func (s *Store) Measurement(database, name string) *Measurement {
	db := s.databaseIndexes[database]
	if db == nil {
		return nil
	}
	return db.measurements[name]
}

func (s *Store) loadIndexes() error {
	dbs, err := ioutil.ReadDir(s.path)
	if err != nil {
		return err
	}
	for _, db := range dbs {
		if !db.IsDir() {
			s.Logger.Printf("Skipping database dir: %s. Not a directory", db.Name())
			continue
		}
		s.databaseIndexes[db.Name()] = NewDatabaseIndex()
	}
	return nil
}

func (s *Store) loadShards() error {
	// loop through the current database indexes
	for db := range s.databaseIndexes {

		rps, err := ioutil.ReadDir(filepath.Join(s.path, db))
		if err != nil {
			return err
		}

		for _, rp := range rps {
			// retention policies should be directories.  Skip anything that is not a dir.
			if !rp.IsDir() {
				s.Logger.Printf("Skipping retention policy dir: %s. Not a directory", rp.Name())
				continue
			}

			shards, err := ioutil.ReadDir(filepath.Join(s.path, db, rp.Name()))
			if err != nil {
				return err
			}
			for _, sh := range shards {
				path := filepath.Join(s.path, db, rp.Name(), sh.Name())

				// Shard file names are numeric shardIDs
				shardID, err := strconv.ParseUint(sh.Name(), 10, 64)
				if err != nil {
					s.Logger.Printf("Skipping shard: %s. Not a valid path", rp.Name())
					continue
				}

				shard := NewShard(s.databaseIndexes[db], path)
				s.shards[shardID] = shard
			}
		}
	}
	return nil

}

func (s *Store) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.shards = map[uint64]*Shard{}
	s.databaseIndexes = map[string]*DatabaseIndex{}

	// Create directory.
	if err := os.MkdirAll(s.path, 0777); err != nil {
		return err
	}

	// TODO: Start AE for Node
	if err := s.loadIndexes(); err != nil {
		return err
	}

	if err := s.loadShards(); err != nil {
		return err
	}

	return nil
}

func (s *Store) WriteToShard(shardID uint64, points []Point) error {
	sh, ok := s.shards[shardID]
	if !ok {
		return ErrShardNotFound
	}
	fmt.Printf("> WriteShard %d, %d points\n", shardID, len(points))

	// Lazily open shards when written.  If the shard is already open,
	// this will do nothing.
	if err := sh.Open(); err != nil {
		return err
	}

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
