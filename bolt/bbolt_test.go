package bolt_test

import (
	"context"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/influxdata/influxdb/bolt"
	"golang.org/x/crypto/bcrypt"
)

func init() {
	bolt.HashCost = bcrypt.MinCost
}

func NewTestClient() (*bolt.Client, func(), error) {
	c, closeFn, err := newTestClient()
	if err != nil {
		return nil, nil, err
	}
	if err := c.Open(context.Background()); err != nil {
		return nil, nil, err
	}

	return c, closeFn, nil
}

func newTestClient() (*bolt.Client, func(), error) {
	c := bolt.NewClient()

	f, err := ioutil.TempFile("", "influxdata-platform-bolt-")
	if err != nil {
		return nil, nil, errors.New("unable to open temporary boltdb file")
	}
	f.Close()

	c.Path = f.Name()

	close := func() {
		c.Close()
		os.Remove(c.Path)
	}

	return c, close, nil
}

func TestClientOpen(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("unable to create temporary test directory %v", err)
	}

	defer func() {
		if err := os.RemoveAll(tempDir); err != nil {
			t.Fatalf("unable to delete temporary test directory %s: %v", tempDir, err)
		}
	}()

	boltFile := filepath.Join(tempDir, "test", "bolt.db")

	c := bolt.NewClient()
	c.Path = boltFile

	if err := c.Open(context.Background()); err != nil {
		t.Fatalf("unable to create database %s: %v", boltFile, err)
	}

	if err := c.Close(); err != nil {
		t.Fatalf("unable to close database %s: %v", boltFile, err)
	}
}

func NewTestKVStore() (*bolt.KVStore, func(), error) {
	f, err := ioutil.TempFile("", "influxdata-platform-bolt-")
	if err != nil {
		return nil, nil, errors.New("unable to open temporary boltdb file")
	}
	f.Close()

	path := f.Name()
	s := bolt.NewKVStore(path)
	if err := s.Open(context.TODO()); err != nil {
		return nil, nil, err
	}

	close := func() {
		s.Close()
		os.Remove(path)
	}

	return s, close, nil
}
