package meta_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	"github.com/influxdb/influxdb/meta"
)

// Ensure the store can be opened and closed.
func TestStore_Open(t *testing.T) {
	path := MustTempFile()
	defer os.RemoveAll(path)

	// Open store in temporary directory.
	s := NewStore()
	if err := s.Open(path); err != nil {
		t.Fatal(err)
	}
	defer s.Close() // idempotent

	// Wait for leadership change.
	select {
	case <-s.LeaderCh():
	case <-time.After(1 * time.Second):
		t.Fatal("no leadership")
	}

	time.Sleep(100 * time.Millisecond)

	// Close store.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

// Ensure the store can create a new node.
func TestStore_CreateNode(t *testing.T) {
	s := MustOpenStore()
	defer s.Close()
	<-s.LeaderCh()

	ni, err := s.CreateNode("host0")
	if err != nil {
		t.Fatal(err)
	} else if *ni != (meta.NodeInfo{ID: 1, Host: "host0"}) {
		t.Fatalf("unexpected node: %#v", ni)
	}
}

// Store is a test wrapper for meta.Store.
type Store struct {
	*meta.Store
	Stderr bytes.Buffer
}

// NewStore returns a new test wrapper for Store.
func NewStore() *Store {
	s := &Store{
		Store: meta.NewStore(),
	}
	s.HeartbeatTimeout = 50 * time.Millisecond
	s.ElectionTimeout = 50 * time.Millisecond
	s.LeaderLeaseTimeout = 50 * time.Millisecond
	s.CommitTimeout = 5 * time.Millisecond
	s.Logger = log.New(&s.Stderr, "", log.LstdFlags)
	return s
}

// MustOpenStore opens a store in a temporary path. Panic on error.
func MustOpenStore() *Store {
	s := NewStore()
	if err := s.Open(MustTempFile()); err != nil {
		panic(err.Error())
	}
	return s
}

func (s *Store) Close() error {
	defer os.RemoveAll(s.Path())
	return s.Store.Close()
}

// MustTempFile returns the path to a non-existent temporary file.
func MustTempFile() string {
	f, _ := ioutil.TempFile("", "influxdb-meta-")
	f.Close()
	os.Remove(f.Name())
	return f.Name()
}

func warn(v ...interface{})              { fmt.Fprintln(os.Stderr, v...) }
func warnf(msg string, v ...interface{}) { fmt.Fprintf(os.Stderr, msg+"\n", v...) }
