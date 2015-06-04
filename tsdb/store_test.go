package tsdb

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestStoreOpen(t *testing.T) {
	dir, err := ioutil.TempDir("", "store_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	if err := os.MkdirAll(filepath.Join(dir, "mydb"), 0600); err != nil {
		t.Fatalf("failed to create test db dir: %v", err)
	}

	s := NewStore(dir)
	if err := s.Open(); err != nil {
		t.Fatalf("Store.Open() failed: %v", err)
	}

	if exp := 1; len(s.databaseIndexes) != exp {
		t.Fatalf("database index count mismatch: got %v, exp %v", len(s.databaseIndexes), exp)
	}
}

func TestStoreOpenShard(t *testing.T) {
	dir, err := ioutil.TempDir("", "store_test")
	if err != nil {
		t.Fatalf("Store.Open() failed to create temp dir: %v", err)
	}

	path := filepath.Join(dir, "mydb", "myrp")
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatalf("Store.Open() failed to create test db dir: %v", err)
	}

	shardPath := filepath.Join(path, "1")
	if _, err := os.Create(shardPath); err != nil {
		t.Fatalf("Store.Open() failed to create test shard 1: %v", err)
	}

	s := NewStore(dir)
	if err := s.Open(); err != nil {
		t.Fatalf("Store.Open() failed: %v", err)
	}

	if exp := 1; len(s.databaseIndexes) != exp {
		t.Fatalf("Store.Open() database index count mismatch: got %v, exp %v", len(s.databaseIndexes), exp)
	}

	if _, ok := s.databaseIndexes["mydb"]; !ok {
		t.Errorf("Store.Open() database myb does not exist")
	}

	if exp := 1; len(s.shards) != exp {
		t.Fatalf("Store.Open() shard count mismatch: got %v, exp %v", len(s.shards), exp)
	}

	sh := s.shards[uint64(1)]
	if sh.path != shardPath {
		t.Errorf("Store.Open() shard path mismatch: got %v, exp %v", sh.path, shardPath)
	}
}

func TestStoreOpenShardCreateDelete(t *testing.T) {
	dir, err := ioutil.TempDir("", "store_test")
	if err != nil {
		t.Fatalf("Store.Open() failed to create temp dir: %v", err)
	}

	path := filepath.Join(dir, "mydb", "myrp")
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatalf("Store.Open() failed to create test db dir: %v", err)
	}

	s := NewStore(dir)
	if err := s.Open(); err != nil {
		t.Fatalf("Store.Open() failed: %v", err)
	}

	if exp := 1; len(s.databaseIndexes) != exp {
		t.Fatalf("Store.Open() database index count mismatch: got %v, exp %v", len(s.databaseIndexes), exp)
	}

	if _, ok := s.databaseIndexes["mydb"]; !ok {
		t.Errorf("Store.Open() database mydb does not exist")
	}

	if err := s.CreateShard("mydb", "myrp", 1); err != nil {
		t.Fatalf("Store.Open() failed to create shard")
	}

	if exp := 1; len(s.shards) != exp {
		t.Fatalf("Store.Open() shard count mismatch: got %v, exp %v", len(s.shards), exp)
	}

	shardIDs := s.ShardIDs()
	if len(shardIDs) != 1 || shardIDs[0] != 1 {
		t.Fatalf("Store.Open() ShardIDs not correct: got %v, exp %v", s.ShardIDs(), []uint64{1})
	}

	if err := s.DeleteShard(1); err != nil {
		t.Fatalf("Store.Open() failed to delete shard: %v", err)
	}

	if _, ok := s.shards[uint64(1)]; ok {
		t.Fatal("Store.Open() shard ID 1 still exists")
	}
}

func TestStoreOpenNotDatabaseDir(t *testing.T) {
	dir, err := ioutil.TempDir("", "store_test")
	if err != nil {
		t.Fatalf("Store.Open() failed to create temp dir: %v", err)
	}

	path := filepath.Join(dir, "bad_db_path")
	if _, err := os.Create(path); err != nil {
		t.Fatalf("Store.Open() failed to create test db dir: %v", err)
	}

	s := NewStore(dir)
	if err := s.Open(); err != nil {
		t.Fatalf("Store.Open() failed: %v", err)
	}

	if exp := 0; len(s.databaseIndexes) != exp {
		t.Fatalf("Store.Open() database index count mismatch: got %v, exp %v", len(s.databaseIndexes), exp)
	}

	if exp := 0; len(s.shards) != exp {
		t.Fatalf("Store.Open() shard count mismatch: got %v, exp %v", len(s.shards), exp)
	}
}

func TestStoreOpenNotRPDir(t *testing.T) {
	dir, err := ioutil.TempDir("", "store_test")
	if err != nil {
		t.Fatalf("Store.Open() failed to create temp dir: %v", err)
	}

	path := filepath.Join(dir, "mydb")
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatalf("Store.Open() failed to create test db dir: %v", err)
	}

	rpPath := filepath.Join(path, "myrp")
	if _, err := os.Create(rpPath); err != nil {
		t.Fatalf("Store.Open() failed to create test retention policy directory: %v", err)
	}

	s := NewStore(dir)
	if err := s.Open(); err != nil {
		t.Fatalf("Store.Open() failed: %v", err)
	}

	if exp := 1; len(s.databaseIndexes) != exp {
		t.Fatalf("Store.Open() database index count mismatch: got %v, exp %v", len(s.databaseIndexes), exp)
	}

	if _, ok := s.databaseIndexes["mydb"]; !ok {
		t.Errorf("Store.Open() database myb does not exist")
	}

	if exp := 0; len(s.shards) != exp {
		t.Fatalf("Store.Open() shard count mismatch: got %v, exp %v", len(s.shards), exp)
	}
}

func TestStoreOpenShardBadShardPath(t *testing.T) {
	dir, err := ioutil.TempDir("", "store_test")
	if err != nil {
		t.Fatalf("Store.Open() failed to create temp dir: %v", err)
	}

	path := filepath.Join(dir, "mydb", "myrp")
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatalf("Store.Open() failed to create test db dir: %v", err)
	}

	// Non-numeric shard ID
	shardPath := filepath.Join(path, "bad_shard_path")
	if _, err := os.Create(shardPath); err != nil {
		t.Fatalf("Store.Open() failed to create test shard 1: %v", err)
	}

	s := NewStore(dir)
	if err := s.Open(); err != nil {
		t.Fatalf("Store.Open() failed: %v", err)
	}

	if exp := 1; len(s.databaseIndexes) != exp {
		t.Fatalf("Store.Open() database index count mismatch: got %v, exp %v", len(s.databaseIndexes), exp)
	}

	if _, ok := s.databaseIndexes["mydb"]; !ok {
		t.Errorf("Store.Open() database myb does not exist")
	}

	if exp := 0; len(s.shards) != exp {
		t.Fatalf("Store.Open() shard count mismatch: got %v, exp %v", len(s.shards), exp)
	}

}
